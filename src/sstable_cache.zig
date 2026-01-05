const std = @import("std");
const global_context = @import("./global_context.zig");

const AppendOnlyQueue = @import("./lock_free.zig").AppendOnlyQueue;
const FileHandle = @import("./data_types.zig").FileHandle;
const RefCounted = @import("./lock_free.zig").RefCounted;
const SSTable = @import("./sstable.zig").SSTable;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;

pub const CacheCleanupTask = struct {
    const Task = @import("./task_queue.zig").Task;

    allocator: std.mem.Allocator,
    cache: *SSTableCache,

    pub fn init(allocator: std.mem.Allocator, cache: *SSTableCache) CacheCleanupTask {
        return .{
            .allocator = allocator,
            .cache = cache,
        };
    }

    pub fn task(self: *CacheCleanupTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
            .enqued_at = std.time.microTimestamp(),
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *CacheCleanupTask = @ptrCast(@alignCast(ptr));
        defer self.cache.cleanup_scheduled.store(false, .release);
        self.cache.cleanupPendingDeletions() catch |e| {
            self.cache.cleanup_scheduled.store(false, .release);
            std.log.err("Error! Failed to cleanup SSTable cache: {any}\n", .{e});
        };
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *CacheCleanupTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

pub const PENDING_DELETION_THRESHOLD = 4;

pub const SSTableCache = struct {
    allocator: std.mem.Allocator,
    capacity: u64,
    size: std.atomic.Value(u64),
    entries: []std.atomic.Value(?*CacheRecord),
    pending_deletions: std.atomic.Value(*PendingDeletionQueue),
    cleanup_scheduled: std.atomic.Value(bool),
    eviction_cursor: std.atomic.Value(u64),

    pub const Handle = struct {
        allocator: std.mem.Allocator,
        table: *SSTable,
        is_deleted: std.atomic.Value(bool),

        pub fn deinit(self: *Handle) void {
            handleCleanup(self.allocator, self);
        }
    };
    pub const CacheRecord = RefCounted(Handle);
    pub const PendingDeletionQueue = AppendOnlyQueue(*CacheRecord, null);

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !SSTableCache {
        var entries = try allocator.alloc(std.atomic.Value(?*CacheRecord), capacity);
        for (0..capacity) |i| {
            entries[i] = std.atomic.Value(?*CacheRecord).init(null);
        }

        const pending_queue = try allocator.create(PendingDeletionQueue);
        pending_queue.* = PendingDeletionQueue.init(allocator);

        return .{
            .allocator = allocator,
            .capacity = capacity,
            .size = std.atomic.Value(u64).init(0),
            .entries = entries,
            .pending_deletions = std.atomic.Value(*PendingDeletionQueue).init(pending_queue),
            .cleanup_scheduled = std.atomic.Value(bool).init(false),
            .eviction_cursor = std.atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *SSTableCache) void {
        for (0..self.capacity) |i| {
            if (self.entries[i].load(.acquire)) |e| {
                e.value.deinit();
                self.allocator.destroy(e);
            }
        }
        self.allocator.free(self.entries);

        const pending_queue = self.pending_deletions.load(.acquire);
        var curr = pending_queue.head.next;
        while (curr) |node| : (curr = node.next) {
            if (node.entry) |record| {
                record.value.deinit();
                self.allocator.destroy(record);
            }
        }
        pending_queue.deinit();
        self.allocator.destroy(pending_queue);
    }

    pub fn get(self: *SSTableCache, handle: FileHandle, file_manager: *TableFileManager) !?*CacheRecord {
        for (0..self.capacity) |i| {
            const record = self.entries[i].load(.acquire) orelse continue;

            _ = record.tryAcquire() orelse continue;
            if (self.entries[i].load(.acquire) != record) {
                _ = record.release();
                continue;
            }

            const record_handle = record.getConst().table.handle;
            if (record_handle.level == handle.level and record_handle.file_id == handle.file_id) {
                if (record.getConst().is_deleted.load(.acquire)) {
                    _ = record.release();
                    return null;
                }
                return record;
            } else if (record.getConst().is_deleted.load(.acquire)) {
                _ = record.release();
                continue;
            }

            _ = record.release();
        }

        const file_list = file_manager.acquireFilesAtLevel(handle.level) orelse return null;
        defer _ = file_list.release();

        var found = false;
        var curr = file_list.get().head;
        while (curr) |node| : (curr = node.next) {
            if (node.entry == handle.file_id) {
                found = true;
                break;
            }
        }
        if (!found) return null;

        if (self.size.load(.acquire) >= self.capacity) {
            self.tryEvictOne();
        }

        const table_ptr = try self.allocator.create(SSTable);
        errdefer self.allocator.destroy(table_ptr);
        table_ptr.* = try SSTable.open(self.allocator, handle);
        errdefer table_ptr.close(true);

        const record = try self.allocator.create(CacheRecord);
        errdefer self.allocator.destroy(record);

        record.* = CacheRecord.init(self.allocator, .{
            .allocator = self.allocator,
            .table = table_ptr,
            .is_deleted = std.atomic.Value(bool).init(false),
        });
        _ = record.acquire();

        const start = self.eviction_cursor.load(.acquire);
        var cached = false;
        for (0..self.capacity) |offset| {
            const i = (start + self.capacity - offset) % self.capacity;
            if (self.entries[i].cmpxchgStrong(null, record, .acq_rel, .acquire)) |_| {
                continue;
            } else {
                _ = self.size.fetchAdd(1, .acq_rel);
                cached = true;
                break;
            }
        }

        if (!cached) {
            _ = record.release();
        }
        return record;
    }

    pub fn cleanupPendingDeletions(self: *SSTableCache) !void {
        const new_queue = try self.allocator.create(PendingDeletionQueue);
        errdefer self.allocator.destroy(new_queue);
        new_queue.* = PendingDeletionQueue.init(self.allocator);

        const old_queue = self.pending_deletions.swap(new_queue, .acq_rel);

        var curr = old_queue.head.next;
        while (curr) |node| : (curr = node.next) {
            if (node.entry) |record| {
                if (record.ref_count.load(.acquire) == 1) {
                    _ = record.release();
                } else {
                    self.pending_deletions.load(.acquire).enqueue(record);
                }
            }
        }

        old_queue.deinit();
        self.allocator.destroy(old_queue);
    }

    fn tryEvictOne(self: *SSTableCache) void {
        const start = self.eviction_cursor.load(.acquire);
        if (!self.tryEvict(start, EvictIfDeleted.condition())) {
            _ = self.tryEvict(start, EvictIfUnused.condition());
        }

        if (self.shouldRunCleanup()) {
            const task_queue = global_context.getTaskQueue();
            var cleanup_task = task_queue.?.allocator.create(CacheCleanupTask) catch |e| {
                std.log.err("Error! Failed to create cleanup task: {any}", .{e});
                return;
            };
            cleanup_task.* = CacheCleanupTask.init(task_queue.?.allocator, self);
            task_queue.?.enqueue(cleanup_task.task());
        }
    }

    const EvictionCondition = struct {
        check_fn: *const fn (cache_record: *const CacheRecord) bool,

        pub inline fn check(self: *const EvictionCondition, cache_record: *const CacheRecord) bool {
            return self.check_fn(cache_record);
        }
    };

    const EvictIfDeleted = struct {
        pub inline fn condition() EvictionCondition {
            return .{
                .check_fn = check,
            };
        }

        fn check(cache_record: *const CacheRecord) bool {
            return cache_record.getConst().is_deleted.load(.acquire);
        }
    };

    const EvictIfUnused = struct {
        pub inline fn condition() EvictionCondition {
            return .{
                .check_fn = check,
            };
        }

        fn check(cache_record: *const CacheRecord) bool {
            return cache_record.ref_count.load(.acquire) == 2;
        }
    };

    fn tryEvict(self: *SSTableCache, start: u64, condition: EvictionCondition) bool {
        for (0..self.capacity) |offset| {
            const idx = (start + offset) % self.capacity;

            if (self.entries[idx].load(.acquire)) |record| {
                _ = record.tryAcquire() orelse continue;
                if (self.entries[idx].load(.acquire) != record) {
                    _ = record.release();
                    continue;
                }

                if (condition.check(record)) {
                    if (self.entries[idx].cmpxchgStrong(record, null, .acq_rel, .acquire) == null) {
                        _ = self.size.fetchSub(1, .acq_rel);
                        self.eviction_cursor.store((idx + 1) % self.capacity, .release);
                        if (record.ref_count.load(.acquire) == 2) {
                            _ = record.release();
                            _ = record.release();
                            return true;
                        } else {
                            self.pending_deletions.load(.acquire).enqueue(record);
                            _ = record.release();
                            return true;
                        }
                    }
                }

                _ = record.release();
            }
        }

        return false;
    }

    fn shouldRunCleanup(self: *SSTableCache) bool {
        if (self.cleanup_scheduled.load(.acquire)) return false;

        const pending_queue = self.pending_deletions.load(.acquire);
        var count: u64 = 0;
        var curr = pending_queue.head.next;
        while (curr) |node| : (curr = node.next) {
            count += 1;
            if (count >= PENDING_DELETION_THRESHOLD) {
                return !self.cleanup_scheduled.swap(true, .acq_rel);
            }
        }
        return false;
    }

    fn handleCleanup(allocator: std.mem.Allocator, handle: *Handle) void {
        handle.table.close(true);
        allocator.destroy(handle.table);
    }
};
