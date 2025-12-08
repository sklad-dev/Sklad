const std = @import("std");

const FileHandle = @import("./data_types.zig").FileHandle;
const RefCounted = @import("./lock_free.zig").RefCounted;
const SSTable = @import("./sstable.zig").SSTable;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;

pub const SSTableCache = struct {
    allocator: std.mem.Allocator,
    capacity: u64,
    size: std.atomic.Value(u64),
    entries: []std.atomic.Value(?*CacheRecord),
    eviction_cursor: std.atomic.Value(u64),

    pub const Handle = struct {
        allocator: std.mem.Allocator,
        table: *SSTable,

        pub fn deinit(self: *Handle) void {
            handleCleanup(self.allocator, self);
        }
    };
    pub const CacheRecord = RefCounted(Handle);

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !SSTableCache {
        var entries = try allocator.alloc(std.atomic.Value(?*CacheRecord), capacity);
        for (0..capacity) |i| {
            entries[i] = std.atomic.Value(?*CacheRecord).init(null);
        }

        return .{
            .allocator = allocator,
            .capacity = capacity,
            .size = std.atomic.Value(u64).init(0),
            .entries = entries,
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
    }

    pub fn get(self: *SSTableCache, handle: FileHandle, file_manager: *TableFileManager) !?*CacheRecord {
        for (0..self.capacity) |i| {
            if (self.entries[i].load(.acquire)) |record| {
                const record_handle = record.getConst().table.handle;
                if (record_handle.level == handle.level and record_handle.file_id == handle.file_id) {
                    var current = record.ref_count.load(.acquire);
                    while (current > 0) {
                        if (record.ref_count.cmpxchgWeak(
                            current,
                            current + 1,
                            .acq_rel,
                            .acquire,
                        )) |updated| {
                            current = updated;
                            continue;
                        }

                        return record;
                    }
                }
            }
        }

        const file_list = file_manager.acquireFilesAtLevel(handle.level) orelse return null;
        defer _ = file_list.release();

        var found = false;
        var curr = file_list.get().head.next;
        while (curr) |node| : (curr = node.next) {
            if (node.entry) |file_id| {
                if (file_id == handle.file_id) {
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            return null;
        }

        if (self.size.load(.acquire) >= self.capacity) {
            self.tryEvictOne();
        }

        const table_value = try SSTable.open(self.allocator, handle);

        const table_ptr = try self.allocator.create(SSTable);
        errdefer self.allocator.destroy(table_ptr);
        table_ptr.* = table_value;

        const handle_value = Handle{
            .allocator = self.allocator,
            .table = table_ptr,
        };

        const record = try self.allocator.create(CacheRecord);
        errdefer self.allocator.destroy(record);
        record.* = CacheRecord.init(self.allocator, handle_value);

        var inserted = false;
        for (0..self.capacity) |i| {
            if (self.entries[i].cmpxchgStrong(
                null,
                record,
                .acq_rel,
                .acquire,
            )) |_| {
                continue;
            } else {
                _ = self.size.fetchAdd(1, .acq_rel);
                inserted = true;
                break;
            }
        }

        _ = record.acquire();
        return record;
    }

    fn tryEvictOne(self: *SSTableCache) void {
        const start = self.eviction_cursor.load(.acquire);

        for (0..self.capacity) |offset| {
            const idx = (start + offset) % self.capacity;

            if (self.entries[idx].load(.acquire)) |record| {
                const current_refs = record.ref_count.load(.acquire);
                if (current_refs == 1) {
                    if (self.entries[idx].cmpxchgStrong(record, null, .acq_rel, .acquire) == null) {
                        _ = self.size.fetchSub(1, .acq_rel);
                        self.eviction_cursor.store((idx + 1) % self.capacity, .release);

                        _ = record.release();
                        return;
                    }
                }
            }
        }

        self.eviction_cursor.store((start + 1) % self.capacity, .release);
    }

    fn handleCleanup(allocator: std.mem.Allocator, handle: *Handle) void {
        handle.table.close(true);
        allocator.destroy(handle.table);
    }
};
