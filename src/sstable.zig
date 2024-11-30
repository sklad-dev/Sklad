const std = @import("std");
const data_types = @import("./data_types.zig");
const m = @import("./memtable.zig");
const b = @import("./bloom.zig");
const utils = @import("./utils.zig");
const ValueType = data_types.ValueType;
const StorageRecord = data_types.StorageRecord;

pub fn SSTable(comptime V: type) type {
    return struct {
        path: []const u8,
        file: std.fs.File,
        bloom_filter: ?b.BloomFilter,
        allocator: std.mem.Allocator,
        min_key: ?[]u8,
        max_key: ?[]u8,
        index_start_offset: u32,
        index_end_offset: u32,

        const Self = @This();

        pub fn create(memtable: *m.Memtable(V), path: []const u8, sparse_index_step: u32, allocator: std.mem.Allocator) !Self {
            const file = try std.fs.cwd().createFile(path, .{
                .read = true,
                .truncate = false,
            });

            var offsets = std.ArrayList(u32).init(allocator);
            defer offsets.deinit();

            var bloom_filter = try b.BloomFilter.init(@intCast(memtable.size), 20, allocator);

            var sstable = Self{
                .path = path,
                .file = file,
                .bloom_filter = bloom_filter,
                .allocator = allocator,
                .min_key = null,
                .max_key = null,
                .index_start_offset = 0,
                .index_end_offset = 0,
            };

            var i: u16 = 0;
            var it = memtable.*.iterator();
            var min_key: []const u8 = undefined;
            var max_key: []const u8 = undefined;
            while (it.next()) |record| {
                if (i == 0) min_key = record.key;
                if (i == memtable.size - 1) max_key = record.key;

                const current_offset: u32 = @intCast(try file.getPos());
                if (current_offset % sparse_index_step == 0) {
                    try offsets.append(current_offset);
                }

                try record.write(sstable.file);
                bloom_filter.add(record.key);
                i += 1;
            }

            const index_start_offset: u32 = @intCast(try file.getPos());
            for (offsets.items) |offset| {
                try utils.write_number(@TypeOf(offset), sstable.file, offset);
            }
            const index_end_offset: u32 = @intCast(try file.getPos());
            sstable.index_start_offset = index_start_offset;
            sstable.index_end_offset = index_end_offset;

            try utils.write_number(u16, sstable.file, @as(u16, @intCast(min_key.len)));
            try file.writeAll(min_key);
            const min_key_buf = try allocator.alloc(u8, min_key.len);
            @memcpy(min_key_buf, min_key);
            sstable.min_key = min_key_buf;

            try utils.write_number(u16, sstable.file, @as(u16, @intCast(max_key.len)));
            try file.writeAll(max_key);
            const max_key_buf = try allocator.alloc(u8, max_key.len);
            @memcpy(max_key_buf, max_key);
            sstable.max_key = max_key_buf;

            const bloom_start_offset: u32 = @intCast(try file.getPos());
            try file.writeAll(bloom_filter.filter);

            try utils.write_number(u32, sstable.file, index_start_offset);
            try utils.write_number(u32, sstable.file, index_end_offset);
            try utils.write_number(u32, sstable.file, bloom_start_offset);

            return sstable;
        }

        pub fn open(path: []const u8, allocator: std.mem.Allocator) !Self {
            const file = try std.fs.cwd().createFile(path, .{
                .read = true,
                .truncate = false,
            });

            try file.seekFromEnd(-4);
            const bloom_start_offset = try utils.read_number(u32, file);

            try file.seekFromEnd(-12);
            const bloom_end_offset: u32 = @intCast(try file.getPos());

            const bloom_size = bloom_end_offset - bloom_start_offset;
            const filter = try allocator.alloc(u8, bloom_size);

            try file.seekFromEnd(-12);
            const index_start_offset: u32 = try utils.read_number(u32, file);
            try file.seekFromEnd(-8);
            const index_end_offset: u32 = try utils.read_number(u32, file);
            try file.seekTo(index_end_offset);
            const min_key_size: u16 = try utils.read_number(u16, file);
            const min_key_buf = try allocator.alloc(u8, min_key_size);
            _ = try file.read(min_key_buf[0..]);
            const max_key_size: u16 = try utils.read_number(u16, file);
            const max_key_buf = try allocator.alloc(u8, max_key_size);
            _ = try file.read(max_key_buf[0..]);

            return Self{
                .path = path,
                .file = file,
                .bloom_filter = b.BloomFilter{
                    .allocator = allocator,
                    .filter = filter,
                },
                .allocator = allocator,
                .min_key = min_key_buf,
                .max_key = max_key_buf,
                .index_start_offset = index_start_offset,
                .index_end_offset = index_end_offset,
            };
        }

        pub inline fn close(self: *Self) void {
            if (self.min_key != null) {
                self.allocator.free(self.min_key.?);
            }
            if (self.max_key != null) {
                self.allocator.free(self.max_key.?);
            }
            if (self.bloom_filter != null) {
                self.bloom_filter.?.deinit();
            }
            self.file.close();
        }

        pub fn find(self: *const Self, key: []const u8) !?V {
            if (self.bloom_filter.?.may_contain(key) == false) return null;
            if (utils.compare_bitwise(key, self.min_key.?) < 0) return null;
            if (utils.compare_bitwise(key, self.max_key.?) > 0) return null;

            const search_result = try self.find_block_offset(key);
            if (search_result < 0) return null;

            const offset: u32 = @as(u32, @intCast(search_result));
            return try self.find_in_block(key, offset);
        }

        fn find_block_offset(self: *const Self, key: []const u8) !i64 {
            var low: u32 = 0;
            var high: u32 = ((self.index_end_offset - self.index_start_offset) / 4 - 1);
            while (low <= high) {
                const high_block_offset: u32 = try self.read_block_offset_from_index(high);
                const high_record = try StorageRecord(V).read_from_offset(self.allocator, self.file, high_block_offset);
                defer self.allocator.free(high_record.key);

                if (utils.compare_bitwise(key, high_record.key) > 0) {
                    return @as(i64, @intCast(high_block_offset));
                }

                const mid = low + (high - low) / 2;
                const block_offset: u32 = try self.read_block_offset_from_index(mid);

                if (mid == low) return @as(i64, @intCast(block_offset));

                const record = try StorageRecord(V).read_from_offset(self.allocator, self.file, block_offset);
                defer self.allocator.free(record.key);

                if (utils.compare_bitwise(key, record.key) == 0) {
                    return @as(i64, @intCast(block_offset));
                } else if (utils.compare_bitwise(key, record.key) < 0) {
                    high = mid;
                } else {
                    low = mid;
                }
            }
            return -1;
        }

        fn find_in_block(self: *const Self, key: []const u8, block_offset: u32) !?V {
            try self.file.seekTo(@as(u64, @intCast(self.index_start_offset + 4)));
            const block_size = try utils.read_number(u32, self.file);
            var offset = block_offset;
            while (offset < block_offset + block_size) {
                const record = try StorageRecord(V).read_from_offset(self.allocator, self.file, offset);
                defer self.allocator.free(record.key);

                if (utils.compare_bitwise(key, record.key) < 0) {
                    return null;
                } else if (utils.compare_bitwise(key, record.key) == 0) {
                    return record.value;
                }
                offset = @as(u32, @intCast(try self.file.getPos()));
            }

            return null;
        }

        inline fn read_block_offset_from_index(self: *const Self, index_offset: u32) !u32 {
            try self.file.seekTo(@intCast(self.index_start_offset + index_offset * 4));
            const block_offset: u32 = try utils.read_number(u32, self.file);
            return block_offset;
        }

        inline fn free_record(self: *const Self, record: *StorageRecord(V)) void {
            self.allocator.free(record.key);
            self.allocator.destroy(record);
        }
    };
}

// Tests
const testing = std.testing;

const TEST_SSTABLE_PATH = "./test.sstable";

fn clean_up(comptime V: type, storage: SSTable(V), memtable: m.Memtable(V)) !void {
    std.fs.cwd().deleteFile(storage.path) catch {
        const out = std.io.getStdOut().writer();
        try std.fmt.format(out, "failed to clean up after the test\n", .{});
    };
    try memtable.wal.delete_file();
}

test "SSTable#create" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = try m.Memtable(u8).init(testing.allocator, rng.random(), 8, 0.125);
    defer test_memtable.destroy();

    const test_vertex_data: u8 = 0;
    for (254..264) |i| {
        try test_memtable.add(&utils.key_from_int_data(usize, i), test_vertex_data);
    }

    var test_sstable = try SSTable(u8).create(&test_memtable, TEST_SSTABLE_PATH, 44, testing.allocator);
    test_sstable.close();
    try clean_up(u8, test_sstable, test_memtable);
}

test "SSTable#open" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = try m.Memtable(u8).init(testing.allocator, rng.random(), 8, 0.125);
    defer test_memtable.destroy();

    const test_vertex_data: u8 = 0;
    for (254..264) |i| {
        try test_memtable.add(&utils.key_from_int_data(usize, i), test_vertex_data);
    }

    var test_sstable = try SSTable(u8).create(&test_memtable, TEST_SSTABLE_PATH, 44, testing.allocator);
    test_sstable.close();

    test_sstable = try SSTable(u8).open(TEST_SSTABLE_PATH, testing.allocator);

    try testing.expect(utils.compare_bitwise(test_sstable.min_key.?, &utils.key_from_int_data(usize, 254)) == 0);
    try testing.expect(utils.compare_bitwise(test_sstable.max_key.?, &utils.key_from_int_data(usize, 263)) == 0);

    test_sstable.close();
    try clean_up(u8, test_sstable, test_memtable);
}

test "SSTable#find" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = try m.Memtable(u8).init(testing.allocator, rng.random(), 8, 0.125);
    defer test_memtable.destroy();

    const test_vertex_data: u8 = 0;
    for (254..264) |i| {
        try test_memtable.add(&utils.key_from_int_data(usize, i), test_vertex_data);
    }

    var test_sstable = try SSTable(u8).create(&test_memtable, TEST_SSTABLE_PATH, 44, testing.allocator);
    test_sstable.close();

    test_sstable = try SSTable(u8).open(TEST_SSTABLE_PATH, testing.allocator);
    defer test_sstable.close();

    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 254)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 256)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 257)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 258)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 259)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 261)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 263)) != null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 200)) == null);
    try testing.expect(try test_sstable.find(&utils.key_from_int_data(usize, 300)) == null);

    try clean_up(u8, test_sstable, test_memtable);
}
