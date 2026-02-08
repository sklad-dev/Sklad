const std = @import("std");
const builtin = @import("builtin");

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const AddOnlyStack = @import("./lock_free.zig").AddOnlyStack;
const CompactionState = @import("./table_file_manager.zig").CompactionState;
const Configurator = @import("./configurator.zig").Configurator;
const Memtable = @import("./memtable.zig").Memtable;
const MemtableIteratorAdapter = @import("./memtable.zig").MemtableIteratorAdapter;
const MergeIterator = @import("./merge_iterator.zig").MergeIterator;
const MetricKind = @import("./metrics.zig").MetricKind;
const RangeQueryContext = @import("./range_query_context.zig").RangeQueryContext;
const RefCounted = @import("./lock_free.zig").RefCounted;
const SSTable = @import("./sstable.zig").SSTable;
const SSTableCache = @import("./sstable_cache.zig").SSTableCache;
const SSTableIteratorAdapter = @import("./sstable.zig").SSTableIteratorAdapter;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Task = @import("./task_queue.zig").Task;
const Wal = @import("./wal.zig").Wal;

const fileNameFromHandle = @import("./sstable.zig").fileNameFromHandle;
const recordMetric = @import("./metrics.zig").recordMetric;
const MANIFEST_ENTRY_SIZE = @import("./manifest.zig").MANIFEST_ENTRY_SIZE;

const BinaryDataRange = data_types.BinaryDataRange;
const FileHandle = data_types.FileHandle;
const FileList = TableFileManager.FileList;
const StorageRecord = data_types.StorageRecord;

const MergeIteratorAdapter = struct {
    merge_iterator: *MergeIterator,

    pub fn init(merge_iterator: *MergeIterator) MergeIteratorAdapter {
        return .{ .merge_iterator = merge_iterator };
    }

    fn nextFn(ctx: *anyopaque) !?StorageRecord {
        const self: *MergeIteratorAdapter = @ptrCast(@alignCast(ctx));
        return self.merge_iterator.next();
    }

    pub fn iterator(self: *MergeIteratorAdapter) StorageRecord.Iterator {
        return .{
            .context = self,
            .next_fn = nextFn,
        };
    }
};

pub const CleanupTask = struct {
    allocator: std.mem.Allocator,
    storage: *BinaryStorage,

    pub fn init(allocator: std.mem.Allocator, storage: *BinaryStorage) !CleanupTask {
        return .{
            .allocator = allocator,
            .storage = storage,
        };
    }

    pub fn task(self: *CleanupTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
            .enqued_at = std.time.microTimestamp(),
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *CleanupTask = @ptrCast(@alignCast(ptr));
        self.doCleanup() catch |e| {
            std.log.err("CleanupTask failed: {any}", .{e});
        };
    }

    fn doCleanup(self: *CleanupTask) !void {
        var last_successful_delete_offset: ?u64 = null;

        var iterator = try self.storage.table_file_manager.manifest.removedFileEntriesIterator();
        while (try iterator.next()) |entry| {
            const handle = FileHandle{ .level = entry.level, .file_id = entry.file_id };
            const cache_record = try self.storage.sstable_cache.get(
                handle,
                &self.storage.table_file_manager,
            );
            if (cache_record) |record| {
                _ = record.release();
                break;
            }

            const file_name = fileNameFromHandle(
                self.allocator,
                self.storage.table_file_manager.path,
                handle,
            ) catch {
                break;
            };
            defer self.allocator.free(file_name);
            std.fs.cwd().deleteFile(file_name) catch {
                break;
            };

            last_successful_delete_offset = iterator.current_offset;
        }

        if (last_successful_delete_offset) |checkpoint_offset| {
            self.storage.table_file_manager.manifest.recordCleanupCheckpoint(checkpoint_offset);
            _ = try self.storage.table_file_manager.manifest.flush();
            self.storage.recordCleanupComplete();
        }
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *CleanupTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

pub const BinaryStorage = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    active_memtable: std.atomic.Value(*Memtable),
    pending_memtables: std.atomic.Value(?*PendingMemtableList),
    swap_in_progress: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    table_file_manager: TableFileManager,
    sstable_cache: SSTableCache,
    configurator: *Configurator,

    last_cleanup_timestamp: std.atomic.Value(i64),
    deleted_files_since_cleanup: std.atomic.Value(u32),

    pub const PendingMemtable = struct {
        id: u64,
        memtable: *Memtable,
        flushed: std.atomic.Value(bool),
    };
    pub const PendingMemtableList = RefCounted(AddOnlyStack(PendingMemtable, pendingMemtableCleanup));

    pub fn pendingMemtableCleanup(allocator: std.mem.Allocator, pm: PendingMemtable) void {
        if (pm.flushed.load(.acquire)) {
            pm.memtable.destroy();
            allocator.destroy(pm.memtable);
        }
    }

    pub const FlushTask = struct {
        allocator: std.mem.Allocator,
        memtable_key: u64,
        storage: *BinaryStorage,

        pub fn init(allocator: std.mem.Allocator, memtable_key: u64, storage: *BinaryStorage) !FlushTask {
            return .{
                .allocator = allocator,
                .memtable_key = memtable_key,
                .storage = storage,
            };
        }

        pub fn task(self: *FlushTask) Task {
            return .{
                .context = self,
                .run_fn = FlushTask.run,
                .destroy_fn = FlushTask.destroy,
                .enqued_at = std.time.microTimestamp(),
            };
        }

        fn run(ptr: *anyopaque) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));

            const list = self.storage.pending_memtables.load(.acquire) orelse return;
            _ = list.acquire();
            defer _ = list.release();

            var memtable: ?*Memtable = null;
            var pending_entry: ?*PendingMemtable = null;
            var curr = list.get().head;
            while (curr) |node| : (curr = node.next) {
                if (node.entry.id == self.memtable_key) {
                    memtable = node.entry.memtable;
                    pending_entry = &node.entry;
                    break;
                }
            }

            if (memtable == null) return;

            self.storage.table_file_manager.flushMemtable(memtable.?) catch |e| {
                std.log.err("Error! Failed to flush a memtable {s}: {any}", .{ memtable.?.wal.path, e });
                return;
            };

            if (pending_entry) |entry| {
                entry.flushed.store(true, .release);
            }

            recordMetric(global_context.getMetricsAggregator(), MetricKind.memtableCounter, 0);

            self.storage.cleanupFlushedMemtables() catch |e| {
                std.log.err("Error! Failed to cleanup flushed memtables: {any}", .{e});
            };

            self.submitCompactionTask();
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn submitCompactionTask(self: *FlushTask) void {
            const threshold = self.storage.configurator.compactionLevelThreshold();
            const files_count = @atomicLoad(u16, &self.storage.table_file_manager.level_counters[0], .acquire);
            if (files_count >= threshold) {
                self.storage.enqueueCompactionTask(0);
            }
        }
    };

    pub const CompactionTask = struct {
        allocator: std.mem.Allocator,
        storage: *BinaryStorage,
        level: u8,

        const CompactionHelper = struct {
            file_ids: []u64,
            adapters: []SSTableIteratorAdapter,
            sstables: []*SSTableCache.CacheRecord,
            iterators: []StorageRecord.Iterator,
            records_number: u32,

            fn init(base_task: *CompactionTask, files: *AddOnlyStack(u64, null), multiplier: usize) !CompactionHelper {
                var file_count: usize = 0;
                var current = files.head;
                while (current) |node| : (current = node.next) {
                    file_count += 1;
                }
                const files_to_compact = @min(file_count, multiplier);

                var all_files = try base_task.allocator.alloc(u64, file_count);
                defer base_task.allocator.free(all_files);

                var i: usize = 0;
                current = files.head;
                while (current) |node| : (current = node.next) {
                    all_files[i] = node.entry;
                    i += 1;
                }

                const files_tail = try base_task.allocator.alloc(u64, files_to_compact);
                const start_idx = file_count - files_to_compact;
                @memcpy(files_tail, all_files[start_idx..]);

                var adapters = try base_task.allocator.alloc(SSTableIteratorAdapter, files_to_compact);
                errdefer base_task.allocator.free(adapters);

                var sstables = try base_task.allocator.alloc(*SSTableCache.CacheRecord, files_to_compact);
                errdefer base_task.allocator.free(sstables);

                var iters = try base_task.allocator.alloc(StorageRecord.Iterator, files_to_compact);
                errdefer base_task.allocator.free(iters);

                var total_records: u32 = 0;
                for (0..files_to_compact) |j| {
                    sstables[j] = (try base_task.storage.sstable_cache.get(.{
                        .level = base_task.level,
                        .file_id = files_tail[j],
                    }, &base_task.storage.table_file_manager)).?;
                    total_records += sstables[j].getConst().table.records_number;
                    adapters[j] = try SSTableIteratorAdapter.init(sstables[j].getConst().table);
                    iters[j] = adapters[j].iterator();
                }

                return .{
                    .file_ids = files_tail,
                    .adapters = adapters,
                    .sstables = sstables,
                    .iterators = iters,
                    .records_number = total_records,
                };
            }

            pub fn deinit(self: *CompactionHelper, allocator: std.mem.Allocator) void {
                for (0..self.adapters.len) |i| {
                    self.adapters[i].sstable_iterator.deinit();
                    self.sstables[i].get().is_deleted.store(true, .release);
                    _ = self.sstables[i].release();
                }

                allocator.free(self.file_ids);
                allocator.free(self.iterators);
                allocator.free(self.sstables);
                allocator.free(self.adapters);
            }
        };

        pub fn init(allocator: std.mem.Allocator, storage: *BinaryStorage, level: u8) !CompactionTask {
            return .{
                .allocator = allocator,
                .storage = storage,
                .level = level,
            };
        }

        pub fn task(self: *CompactionTask) Task {
            return .{
                .context = self,
                .run_fn = CompactionTask.run,
                .destroy_fn = CompactionTask.destroy,
                .enqued_at = std.time.microTimestamp(),
            };
        }

        fn do_run(self: *CompactionTask) !void {
            const multiplier = self.storage.configurator.compactionLevelMultiplier();

            const files = self.storage.table_file_manager.acquireFilesAtLevel(self.level) orelse return;
            defer _ = files.release();

            var helper = try CompactionHelper.init(self, files.get(), multiplier);
            defer helper.deinit(self.allocator);

            var merge_iter = try MergeIterator.init(
                self.allocator,
                helper.iterators,
            );
            defer merge_iter.deinit();

            var adapter = MergeIteratorAdapter.init(&merge_iter);
            var records_merge_iterator = adapter.iterator();

            const file_id = self.storage.table_file_manager.nextFileIdForLevel(self.level + 1);

            var sstable = try SSTable.create(
                self.allocator,
                &records_merge_iterator,
                helper.records_number,
                .{ .level = self.level + 1, .file_id = file_id },
                self.storage.configurator.sstableBlockSize(),
                self.storage.configurator.sstableBloomBitsPerKey(),
            );
            defer sstable.close(false);

            try self.storage.table_file_manager.addFileAtLevel(self.level + 1, sstable.handle.file_id);
            try self.storage.table_file_manager.deleteFilesAtLevel(self.level, helper.file_ids);
            self.storage.incrementDeletedFilesBy(@intCast(helper.file_ids.len));
        }

        fn run(ptr: *anyopaque) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));

            if (@cmpxchgWeak(
                u8,
                &self.storage.table_file_manager.compaction_flags[self.level],
                @intFromEnum(CompactionState.scheduled),
                @intFromEnum(CompactionState.running),
                .seq_cst,
                .seq_cst,
            ) != null) {
                return;
            }

            const trace = @errorReturnTrace();
            self.do_run() catch |e| {
                _ = @cmpxchgWeak(
                    u8,
                    &self.storage.table_file_manager.compaction_flags[self.level],
                    @intFromEnum(CompactionState.running),
                    @intFromEnum(CompactionState.none),
                    .seq_cst,
                    .seq_cst,
                );

                std.log.err("Error! Compaction task at level {d} failed: {any}", .{ self.level, e });
                if (trace) |t| {
                    std.log.err("CompactionTask stack trace:", .{});
                    std.debug.dumpStackTrace(t.*);
                }
                return;
            };

            const threshold = self.storage.configurator.compactionLevelThreshold();
            const max_level = self.storage.configurator.compactionMaxLevel();

            _ = @cmpxchgWeak(
                u8,
                &self.storage.table_file_manager.compaction_flags[self.level],
                @intFromEnum(CompactionState.running),
                @intFromEnum(CompactionState.none),
                .seq_cst,
                .seq_cst,
            );

            const files_count = @atomicLoad(u16, &self.storage.table_file_manager.level_counters[self.level + 1], .acquire);
            if (self.level + 1 < max_level and files_count >= threshold) {
                self.storage.enqueueCompactionTask(self.level + 1);
            }
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }
    };

    pub fn start(allocator: std.mem.Allocator, path: []const u8) !BinaryStorage {
        var deleted_files = std.AutoHashMap(FileHandle, void).init(allocator);
        defer deleted_files.deinit();

        var table_file_manager = try TableFileManager.init(allocator, path, &deleted_files);
        const memtable = try restoreMemtables(&table_file_manager);
        var storage = BinaryStorage{
            .allocator = allocator,
            .path = path,
            .active_memtable = std.atomic.Value(*Memtable).init(memtable),
            .pending_memtables = std.atomic.Value(?*PendingMemtableList).init(null),
            .table_file_manager = table_file_manager,
            .sstable_cache = undefined,
            .configurator = global_context.getConfigurator().?,
            .last_cleanup_timestamp = std.atomic.Value(i64).init(0),
            .deleted_files_since_cleanup = std.atomic.Value(u32).init(@intCast(deleted_files.count())),
        };
        storage.sstable_cache = try SSTableCache.init(allocator, storage.configurator.sstableCacheSize());
        return storage;
    }

    pub inline fn stop(self: *BinaryStorage) void {
        self.deinitMemtables();
        self.table_file_manager.deinit();
        self.sstable_cache.deinit();
    }

    pub fn put(self: *BinaryStorage, key: []const u8, value: []const u8, timestamp: i64, ttl: ?i64) !void {
        const record = StorageRecord.init(key, value, timestamp, ttl);

        var filled_memtable: ?*Memtable = null;
        var filled_memtable_key: u64 = undefined;
        var memtable: *Memtable = undefined;
        var data_slot: ?Memtable.ReservedDataSlot = null;

        const payload_size: u64 = @intCast(record.sizeInMemory());

        while (true) {
            memtable = self.active_memtable.load(.acquire);

            data_slot = memtable.reserve(payload_size);
            if (data_slot) |_| break;

            const was_swapping = self.swap_in_progress.swap(true, .acq_rel);
            if (!was_swapping) {
                const current = self.active_memtable.load(.acquire);
                if (current == memtable and memtable.reserve(payload_size) == null) {
                    const new_memtable = try Memtable.create(self.allocator, self.path);
                    const memtable_key: u64 = std.crypto.random.int(u64);

                    try self.addPendingMemtable(memtable_key, current);

                    self.active_memtable.store(new_memtable, .release);
                    filled_memtable = current;
                    filled_memtable_key = memtable_key;

                    recordMetric(global_context.getMetricsAggregator(), MetricKind.memtableCounter, 1);
                }
                self.swap_in_progress.store(false, .release);
                continue;
            } else {
                std.atomic.spinLoopHint();
                continue;
            }
        }

        try memtable.wal.writeRecord(&record);
        try memtable.add(&record.key, &record.value, &(data_slot.?));

        if (filled_memtable) |_| {
            const task_queue = global_context.getTaskQueue();
            var flush_task = try task_queue.?.allocator.create(FlushTask);

            flush_task.* = FlushTask{
                .allocator = self.allocator,
                .memtable_key = filled_memtable_key,
                .storage = self,
            };

            task_queue.?.enqueue(flush_task.task());
        }
    }

    pub fn delete(self: *BinaryStorage, key: []const u8, timestamp: i64) !void {
        return self.put(key, &[_]u8{}, timestamp, null);
    }

    pub fn find(self: *BinaryStorage, key: []const u8) !?[]const u8 {
        const active = self.active_memtable.load(.acquire);

        var value = active.find(key);
        if (value) |v| {
            if (v.len == 0) return null;

            const result = try self.allocator.alloc(u8, v.len);
            @memcpy(result, v);
            return result;
        }

        if (self.pending_memtables.load(.acquire)) |list| {
            _ = list.acquire();
            defer _ = list.release();

            var curr = list.get().head;
            while (curr) |node| : (curr = node.next) {
                value = node.entry.memtable.find(key);
                if (value) |v| {
                    if (v.len == 0) return null;

                    const result = try self.allocator.alloc(u8, v.len);
                    @memcpy(result, v);
                    return result;
                }
            }
        }

        const result = try self.findInTables(key);
        if (result != null and result.?.len > 0) {
            return result;
        }

        return null;
    }

    pub fn findInRange(self: *BinaryStorage, start_key: []const u8, end_key: []const u8) !RangeQueryContext {
        return RangeQueryContext.init(
            self.allocator,
            self,
            .{
                .start = start_key,
                .end = end_key,
            },
        );
    }

    pub fn enqueueCompactionTask(self: *BinaryStorage, level: u8) void {
        const task_queue = global_context.getTaskQueue();

        if (@cmpxchgWeak(
            u8,
            &self.table_file_manager.compaction_flags[level],
            @intFromEnum(CompactionState.none),
            @intFromEnum(CompactionState.scheduled),
            .seq_cst,
            .seq_cst,
        ) == null) {
            var compaction_task = task_queue.?.allocator.create(CompactionTask) catch |e| {
                std.log.err("Error! Failed to create compaction task: {any}", .{e});
                return;
            };

            compaction_task.* = CompactionTask{
                .allocator = self.allocator,
                .storage = self,
                .level = level,
            };

            task_queue.?.enqueue(compaction_task.task());
        }

        if (self.shouldRunCleanup()) {
            var cleanup_task = task_queue.?.allocator.create(CleanupTask) catch |e| {
                std.log.err("Error! Failed to create cleanup task: {any}", .{e});
                return;
            };
            cleanup_task.* = try CleanupTask.init(task_queue.?.allocator, self);
            task_queue.?.enqueue(cleanup_task.task());
        }
    }

    pub fn shouldRunCleanup(self: *BinaryStorage) bool {
        const now = std.time.microTimestamp();
        const last_cleanup = self.last_cleanup_timestamp.load(.acquire);
        const deleted_count = self.deleted_files_since_cleanup.load(.acquire);

        const time_threshold = self.configurator.cleanupIntervalSeconds() * std.time.us_per_s;
        const count_threshold = self.configurator.cleanupFileCountThreshold();

        const time_elapsed = now - last_cleanup;
        return time_elapsed >= time_threshold and deleted_count >= count_threshold;
    }

    pub fn recordCleanupComplete(self: *BinaryStorage) void {
        self.last_cleanup_timestamp.store(std.time.microTimestamp(), .release);
        self.deleted_files_since_cleanup.store(0, .release);
    }

    pub fn incrementDeletedFilesBy(self: *BinaryStorage, count: u32) void {
        _ = self.deleted_files_since_cleanup.fetchAdd(count, .acq_rel);
    }

    fn addPendingMemtable(self: *BinaryStorage, id: u64, memtable: *Memtable) !void {
        const pending = PendingMemtable{
            .id = id,
            .memtable = memtable,
            .flushed = std.atomic.Value(bool).init(false),
        };

        var old_stack = self.pending_memtables.load(.acquire);

        if (old_stack == null) {
            const new_stack = try self.allocator.create(PendingMemtableList);
            new_stack.* = PendingMemtableList.init(self.allocator, AddOnlyStack(PendingMemtable, pendingMemtableCleanup).init(self.allocator));

            if (self.pending_memtables.cmpxchgStrong(null, new_stack, .acq_rel, .acquire)) |existing| {
                new_stack.get().deinit();
                self.allocator.destroy(new_stack);
                old_stack = existing;
            } else {
                old_stack = new_stack;
            }
        }

        old_stack.?.get().push(pending);
    }

    fn findInTables(self: *BinaryStorage, key: []const u8) !?[]const u8 {
        const max_level = self.configurator.compactionMaxLevel();
        for (0..max_level) |level| {
            var files = self.table_file_manager.acquireFilesAtLevel(@intCast(level)) orelse continue;
            defer _ = files.release();

            var current = files.get().head;
            while (current) |node| : (current = node.next) {
                var cached_record = try self.sstable_cache.get(.{
                    .level = @intCast(level),
                    .file_id = node.entry,
                }, &self.table_file_manager) orelse continue;
                defer _ = cached_record.release();
                if (try cached_record.getConst().table.find(key)) |value| {
                    return value;
                }
            }
        }
        return null;
    }

    fn restoreMemtables(table_file_manager: *TableFileManager) !*Memtable {
        var dir = try std.fs.cwd().openDir(table_file_manager.path, .{
            .access_sub_paths = false,
            .iterate = true,
            .no_follow = true,
        });
        defer dir.close();

        var memtable = try Memtable.create(table_file_manager.allocator, table_file_manager.path);

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".wal") and !std.mem.eql(u8, memtable.wal.name(), entry.name)) {
                const wal_name = try table_file_manager.allocator.alloc(u8, table_file_manager.path.len + 9);
                const wal = try Wal.open(
                    table_file_manager.allocator,
                    try std.fmt.bufPrint(
                        wal_name,
                        "{s}/{s}",
                        .{ table_file_manager.path, entry.name },
                    ),
                );
                while (try Memtable.fromWal(wal, memtable) == true) {
                    try table_file_manager.flushMemtable(memtable);
                    memtable.destroy();
                    table_file_manager.allocator.destroy(memtable);
                    memtable = try Memtable.create(table_file_manager.allocator, table_file_manager.path);
                }
                wal.file.close();
                try wal.deleteFile();
                wal.allocator.free(wal.path);
            }
        }

        recordMetric(global_context.getMetricsAggregator(), MetricKind.memtableCounter, 1);

        return memtable;
    }

    fn cleanupFlushedMemtables(self: *BinaryStorage) !void {
        _ = self.pending_memtables.load(.acquire) orelse return;

        const new_stack = try self.allocator.create(PendingMemtableList);
        new_stack.* = PendingMemtableList.init(self.allocator, AddOnlyStack(PendingMemtable, pendingMemtableCleanup).init(self.allocator));
        errdefer {
            new_stack.get().deinit();
            self.allocator.destroy(new_stack);
        }

        const old_stack = self.pending_memtables.swap(new_stack, .acq_rel);
        if (old_stack == null) return;

        _ = old_stack.?.acquire();
        defer _ = old_stack.?.release();

        var curr = old_stack.?.get().head;
        while (curr) |node| : (curr = node.next) {
            if (!node.entry.flushed.load(.acquire)) {
                new_stack.get().push(node.entry);
            }
        }

        _ = old_stack.?.release();
    }

    fn deinitMemtables(self: *BinaryStorage) void {
        const active = self.active_memtable.load(.acquire);
        active.destroy();
        self.allocator.destroy(active);

        if (self.pending_memtables.load(.acquire)) |list| {
            var curr = list.get().head;
            while (curr) |node| : (curr = node.next) {
                pendingMemtableCleanup(self.allocator, node.entry);
            }
            _ = list.release();
        }
    }
};

// Tests
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

fn cleanup(storage: *BinaryStorage) !void {
    try std.fs.cwd().deleteFile("./MANIFEST");
    try storage.active_memtable.load(.unordered).wal.deleteFile();

    if (storage.pending_memtables.load(.acquire)) |list| {
        _ = list.acquire();
        defer _ = list.release();

        var curr = list.get().head;
        while (curr) |node| : (curr = node.next) {
            try node.entry.memtable.wal.deleteFile();
        }
    }

    for (0..storage.table_file_manager.files.len) |level| {
        if (storage.table_file_manager.acquireFilesAtLevel(@intCast(level))) |files| {
            defer _ = files.release();

            var current = files.get().head;
            while (current) |node| : (current = node.next) {
                var cached_record = try storage.sstable_cache.get(.{
                    .level = @intCast(level),
                    .file_id = node.entry,
                }, &storage.table_file_manager) orelse continue;
                defer _ = cached_record.release();
            }
        }
    }

    var dir = try std.fs.cwd().openDir("./", .{
        .access_sub_paths = false,
        .iterate = true,
        .no_follow = true,
    });
    defer dir.close();

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".sstable")) {
            try std.fs.cwd().deleteFile(entry.name);
        }
    }
}

test "BinaryStorage#put" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(2304, 2, 96);
    defer global_context.deinitConfigurationForTests();

    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var test_storage = try BinaryStorage.start(testing.allocator, ".");
    defer test_storage.stop();

    try test_storage.put(&utils.intToBytes(u8, 1), &utils.intToBytes(u8, 42), std.time.milliTimestamp(), null);
    try testing.expect(test_storage.active_memtable.load(.unordered).size == 1);
    const result = test_storage.active_memtable.load(.unordered).find(&utils.intToBytes(u8, 1));
    try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u8, 42)));

    try test_storage.active_memtable.load(.unordered).wal.deleteFile();

    try std.fs.cwd().deleteFile("MANIFEST");
}

test "Restore memtable from wal" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(2304, 2, 96);
    defer global_context.deinitConfigurationForTests();

    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var storage1 = try BinaryStorage.start(testing.allocator, ".");
    try storage1.put(&utils.intToBytes(u8, 3), &utils.intToBytes(u16, 0xCFCF), std.time.milliTimestamp(), null);
    try storage1.put(&utils.intToBytes(u8, 4), &utils.intToBytes(u16, 0xFAFA), std.time.milliTimestamp(), null);
    storage1.stop();

    var storage2 = try BinaryStorage.start(testing.allocator, ".");
    defer storage2.stop();

    try testing.expect(storage2.active_memtable.load(.unordered).size == 2);

    var result = storage2.active_memtable.load(.unordered).find(&utils.intToBytes(u8, 3));
    try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u16, 0xCFCF)));

    result = storage2.active_memtable.load(.unordered).find(&utils.intToBytes(u8, 4));
    try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u16, 0xFAFA)));

    try storage2.active_memtable.load(.unordered).wal.deleteFile();
    try std.fs.cwd().deleteFile("MANIFEST");
}

test "BinaryStorage#find" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(2304, 2, 96);
    defer global_context.deinitConfigurationForTests();

    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    const value = utils.intToBytes(u8, 42);
    try storage.put(&utils.intToBytes(u8, 1), &value, std.time.milliTimestamp(), null);

    var search_result = try storage.find(&utils.intToBytes(u8, 1));
    try testing.expect(std.mem.eql(u8, search_result.?, &value));
    testing.allocator.free(search_result.?);

    search_result = try storage.find(&utils.intToBytes(u8, 2));
    try testing.expect(search_result == null);

    for (2..10) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v), std.time.milliTimestamp(), null);
    }

    for (1..10) |i| {
        search_result = try storage.find(&utils.intToBytes(u8, @as(u8, @intCast(i))));
        defer testing.allocator.free(search_result.?);
        try testing.expect(search_result != null);
    }

    try cleanup(&storage);
}

test "BinaryStorage#find returns the newest value" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(2304, 2, 96);
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v), std.time.milliTimestamp(), null);
    }

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v * 2), std.time.milliTimestamp(), null);
    }

    var search_result: ?[]const u8 = null;
    for (0..8) |i| {
        search_result = try storage.find(&utils.intToBytes(u8, @as(u8, @intCast(i))));
        defer testing.allocator.free(search_result.?);
        try testing.expect(std.mem.eql(u8, search_result.?, &utils.intToBytes(u8, @intCast(i * 2))));
    }

    try cleanup(&storage);
}

test "BinaryStorage#delete when in memtable" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(2304, 2, 96);
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v), std.time.milliTimestamp(), null);
    }

    try storage.delete(&utils.intToBytes(u8, 3), std.time.milliTimestamp());
    const search_result = try storage.find(&utils.intToBytes(u8, @as(u8, @intCast(3))));
    try testing.expect(search_result == null);

    try cleanup(&storage);
}

test "BinaryStorage#delete when in sstable" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 104;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    const deleted_key = 4;
    {
        var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 4096, 8, "./");
        defer test_memtable.destroy();

        const test_value = utils.intToBytes(u8, 255);
        var slot: ?Memtable.ReservedDataSlot = null;
        for (0..10) |j| {
            slot = test_memtable.reserve(22);
            try test_memtable.add(
                &.{
                    .data = &utils.intToBytes(usize, j),
                    .timestamp = std.time.milliTimestamp(),
                },
                &.{ .data = &test_value, .flags = 0, .ttl = null },
                &slot.?,
            );
        }

        slot = test_memtable.reserve(20);
        try test_memtable.add(
            &.{
                .data = &utils.intToBytes(usize, deleted_key),
                .timestamp = std.time.milliTimestamp(),
            },
            &.{ .data = data_types.EMPTY_VALUE, .flags = null, .ttl = null },
            &slot.?,
        );

        var adapter = MemtableIteratorAdapter.init(&test_memtable);
        var memtable_iterator = adapter.iterator();

        var test_sstable = try SSTable.create(
            testing.allocator,
            &memtable_iterator,
            test_memtable.size,
            .{ .level = 0, .file_id = @as(u64, 0) },
            block_size,
            20,
        );
        test_sstable.close(false);
        try test_memtable.wal.deleteFile();
    }

    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(4096, 2, block_size);
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    var search_result = try storage.find(&utils.intToBytes(usize, deleted_key));
    try testing.expect(search_result == null);

    search_result = try storage.find(&utils.intToBytes(usize, 11));
    try testing.expect(search_result == null);

    try cleanup(&storage);
}

test "BinaryStorage compaction and cleanup" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 108;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    for (0..5) |i| {
        var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 4096, 8, "./");
        defer test_memtable.destroy();

        const test_value = utils.intToBytes(u8, 255);
        var slot: ?Memtable.ReservedDataSlot = null;
        for (0..10) |j| {
            const record = StorageRecord.init(
                &utils.intToBytes(usize, j + i * 10),
                &test_value,
                std.time.milliTimestamp(),
                null,
            );
            slot = test_memtable.reserve(record.sizeInMemory());
            try test_memtable.add(&record.key, &record.value, &slot.?);
        }

        var adapter = MemtableIteratorAdapter.init(&test_memtable);
        var memtable_iterator = adapter.iterator();

        var test_sstable = try SSTable.create(
            testing.allocator,
            &memtable_iterator,
            test_memtable.size,
            .{ .level = 0, .file_id = @as(u64, i) },
            block_size,
            20,
        );
        test_sstable.close(false);
        try test_memtable.wal.deleteFile();
    }

    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(4096, 2, block_size);
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");

    var file_list = storage.table_file_manager.acquireFilesAtLevel(0);
    var curr = file_list.?.get().head;
    var expected_file_id: i64 = 4;
    while (curr) |node| : (curr = node.next) {
        try testing.expect(node.entry == @as(u64, @intCast(expected_file_id)));
        expected_file_id -= 1;
    }
    _ = file_list.?.release();

    for (0..50) |i| {
        const r = try storage.find(&utils.intToBytes(usize, i));
        defer testing.allocator.free(r.?);
        try testing.expect(std.mem.eql(u8, r.?, &utils.intToBytes(u8, 255)));
    }

    _ = @cmpxchgWeak(
        u8,
        &storage.table_file_manager.compaction_flags[0],
        @intFromEnum(CompactionState.none),
        @intFromEnum(CompactionState.scheduled),
        .seq_cst,
        .seq_cst,
    );
    var compaction_task = BinaryStorage.CompactionTask{
        .allocator = testing.allocator,
        .storage = &storage,
        .level = 0,
    };
    compaction_task.task().run_fn(&compaction_task);

    file_list = storage.table_file_manager.acquireFilesAtLevel(0);
    curr = file_list.?.get().head;
    while (curr) |node| : (curr = node.next) {
        try testing.expect(node.entry != 0);
        try testing.expect(node.entry != 1);
        try testing.expect(node.entry != 2);
        try testing.expect(node.entry != 3);
    }
    _ = file_list.?.release();

    for (0..50) |i| {
        const r = try storage.find(&utils.intToBytes(usize, i));
        try testing.expect(std.mem.eql(u8, r.?, &utils.intToBytes(u8, 255)));
        testing.allocator.free(r.?);
    }

    var test_sstable = try SSTable.open(testing.allocator, .{ .level = 1, .file_id = 0 });
    defer test_sstable.close(true);

    try testing.expect(std.mem.eql(u8, test_sstable.min_key.?, &utils.intToBytes(usize, 0)));
    try testing.expect(std.mem.eql(u8, test_sstable.max_key.?, &utils.intToBytes(usize, 39)));
    try testing.expect(test_sstable.records_number == 40);
    try testing.expect(test_sstable.block_size == block_size);
    try testing.expect(test_sstable.index_records_num == 10);
    storage.stop();

    storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    curr = storage.table_file_manager.acquireFilesAtLevel(0).?.get().head;
    while (curr) |node| : (curr = node.next) {
        try testing.expect(node.entry != 0);
        try testing.expect(node.entry != 1);
        try testing.expect(node.entry != 2);
        try testing.expect(node.entry != 3);
    }

    for (0..50) |i| {
        const r = try storage.find(&utils.intToBytes(usize, i));
        try testing.expect(std.mem.eql(u8, r.?, &utils.intToBytes(u8, 255)));
        testing.allocator.free(r.?);
    }

    var cleanup_task = try CleanupTask.init(testing.allocator, &storage);
    try cleanup_task.doCleanup();
    for (0..4) |i| {
        const file_name = try fileNameFromHandle(testing.allocator, ".", .{ .level = 0, .file_id = i });
        defer testing.allocator.free(file_name);
        try testing.expectError(error.FileNotFound, std.fs.cwd().access(file_name, .{}));
    }

    var reader_buffer: [8]u8 = undefined;
    var reader = storage.table_file_manager.manifest.file.reader(&reader_buffer);
    try reader.seekTo(try storage.table_file_manager.manifest.file.getEndPos() - 16);
    const offset = try utils.readNumber(u64, &reader.interface);
    try testing.expect(offset == 0x5A);

    try cleanup(&storage);
}

test "BinaryStorage#findInRange" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 108;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    for (0..5) |i| {
        var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 4096, 8, "./");
        defer test_memtable.destroy();

        const test_value = utils.intToBytes(u8, 255);
        var slot: ?Memtable.ReservedDataSlot = null;
        for (0..10) |j| {
            const record = StorageRecord.init(
                &utils.intToBytes(usize, j + i * 10),
                &test_value,
                std.time.milliTimestamp(),
                null,
            );
            slot = test_memtable.reserve(record.sizeInMemory());
            try test_memtable.add(&record.key, &record.value, &slot.?);
        }

        var adapter = MemtableIteratorAdapter.init(&test_memtable);
        var memtable_iterator = adapter.iterator();

        var test_sstable = try SSTable.create(
            testing.allocator,
            &memtable_iterator,
            test_memtable.size,
            .{ .level = 0, .file_id = @as(u64, i) },
            block_size,
            20,
        );
        test_sstable.close(false);
        try test_memtable.wal.deleteFile();
    }

    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init(4096, 2, block_size);
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    for (0..10) |k| {
        try storage.put(
            &utils.intToBytes(usize, k),
            &utils.intToBytes(u8, @as(u8, @intCast(k))),
            std.time.milliTimestamp(),
            null,
        );
    }

    var ri1 = try storage.findInRange(&utils.intToBytes(usize, 5), &utils.intToBytes(usize, 15));
    defer ri1.deinit();
    var expected_key: usize = 5;
    while (try ri1.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key.data, &utils.intToBytes(usize, expected_key)));
        expected_key += 1;
    }

    var ri2 = try storage.findInRange(&utils.intToBytes(usize, 0), &utils.intToBytes(usize, 59));
    defer ri2.deinit();
    expected_key = 0;
    while (try ri2.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key.data, &utils.intToBytes(usize, expected_key)));
        expected_key += 1;
    }

    try cleanup(&storage);
}
