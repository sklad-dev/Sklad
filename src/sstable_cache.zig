const std = @import("std");
const SSTable = @import("./sstable.zig").SSTable;
const AppendDeleteList = @import("./lock_free.zig").AppendDeleteList;

pub const Handle = struct {
    allocator: std.mem.Allocator,
    ref_count: u32,
    last_epoch: u64,
    cached: bool,
    hash: u64,
    cache: *SSTableCache,
    table: *SSTable,

    pub fn release(self: *Handle) void {
        if (self.cached) {
            _ = @atomicRmw(u32, &self.ref_count, .Sub, 1, .acq_rel);
        } else {
            self.table.close(true);
            self.allocator.destroy(self.table);
        }
    }
};

pub const SSTableCache = struct {
    allocator: std.mem.Allocator,
    capacity: u64,
    size: u64,
    epoch: u64,
    entries: AppendDeleteList(Handle, *Handle),

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !SSTableCache {
        return .{
            .allocator = allocator,
            .capacity = capacity,
            .size = 0,
            .epoch = 1,
            .entries = try AppendDeleteList(Handle, *Handle).init(allocator, handleCleanup),
        };
    }

    pub fn deinit(self: *SSTableCache) void {
        self.entries.deinit();
    }

    pub fn get(self: *SSTableCache, path: []const u8) !*Handle {
        const h = hashPath(path);
        const now = @atomicRmw(u64, &self.epoch, .Add, 1, .acq_rel) + 1;

        {
            var iter = self.entries.iterator();
            defer iter.deinit();

            while (iter.next()) |node| {
                const e = node.entry.?;
                if (e.hash == h and std.mem.eql(u8, e.table.path, path)) {
                    _ = @atomicRmw(u32, &e.ref_count, .Add, 1, .acq_rel);
                    @atomicStore(u64, &e.last_epoch, now, .release);
                    return e;
                }
            }
        }

        if (@atomicLoad(u64, &self.size, .acquire) >= self.capacity) {
            self.tryEvictOne();
        }

        const table_value = try SSTable.open(self.allocator, path);
        const e = try self.allocator.create(Handle);
        errdefer self.allocator.destroy(e);

        const table_ptr = try self.allocator.create(SSTable);
        errdefer self.allocator.destroy(table_ptr);

        table_ptr.* = table_value;

        e.* = .{
            .allocator = self.allocator,
            .ref_count = 1,
            .last_epoch = now,
            .cached = true,
            .hash = h,
            .cache = self,
            .table = table_ptr,
        };

        if (self.entries.prepend(e)) {
            _ = @atomicRmw(u64, &self.size, .Add, 1, .acq_rel);
            return e;
        } else |_| {
            e.cached = false;
            return e;
        }
    }

    fn tryEvictOne(self: *SSTableCache) void {
        var iter = self.entries.iterator();
        defer iter.deinit();

        var candidate: ?*Handle = null;
        var candidate_epoch: u64 = std.math.maxInt(u64);

        while (iter.next()) |node| {
            const e = node.entry.?;
            if (@atomicLoad(u32, &e.ref_count, .acquire) == 1) {
                const last_epoch = @atomicLoad(u64, &e.last_epoch, .acquire);
                if (last_epoch < candidate_epoch) {
                    candidate_epoch = last_epoch;
                    candidate = e;
                }
            }
        }

        if (candidate) |ptr| {
            self.entries.markDelete(matchPtr, ptr);
        }
    }

    inline fn hashPath(path: []const u8) usize {
        var wy = std.hash.Wyhash.init(0);
        wy.update(path);
        return wy.final();
    }

    fn handleCleanup(allocator: std.mem.Allocator, handle: *Handle) void {
        handle.table.close(true);
        allocator.destroy(handle.table);
        allocator.destroy(handle);
    }

    fn matchPtr(entry: ?*Handle, expected: *Handle) bool {
        return entry != null and entry.? == expected;
    }
};
