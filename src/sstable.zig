const std = @import("std");
const data_types = @import("./data_types.zig");
const m = @import("./memtable.zig");
const b = @import("./bloom.zig");
const utils = @import("./utils.zig");
const ValueType = data_types.ValueType;
const NodeRecord = data_types.NodeRecord;

pub const SSTable = struct {
    path: []const u8,
    file: std.fs.File,
    bloom_filter: ?b.BloomFilter,
    allocator: std.mem.Allocator,
    min_value: ?[]u8,
    max_value: ?[]u8,
    index_start_offset: u32,
    index_end_offset: u32,

    pub fn create(comptime N: u8, memtable: *m.Memtable(N), path: []const u8, sparse_index_step: u32, allocator: std.mem.Allocator) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        var offsets = std.ArrayList(u32).init(allocator);
        defer offsets.deinit();

        var bloom_filter = try b.BloomFilter.init(@intCast(memtable.size), 20, allocator);

        var sstable = SSTable{
            .path = path,
            .file = file,
            .bloom_filter = bloom_filter,
            .allocator = allocator,
            .min_value = null,
            .max_value = null,
            .index_start_offset = 0,
            .index_end_offset = 0,
        };

        var i: u16 = 0;
        var it = memtable.*.iterator();
        var min_key: []u8 = undefined;
        var max_key: []u8 = undefined;
        while (it.next()) |node| {
            if (i == 0) min_key = node.key.?;
            if (i == memtable.size - 1) max_key = node.key.?;

            const current_offset: u32 = @intCast(try file.getPos());
            if (current_offset % sparse_index_step == 0) {
                try offsets.append(current_offset);
            }
            try sstable.write(@TypeOf(node.value.?.node_id), node.value.?.node_id);
            const value_type = @intFromEnum(node.value.?.value_type);
            try sstable.write(@TypeOf(value_type), value_type);
            try sstable.write(@TypeOf(node.value.?.value_size), node.value.?.value_size);
            try file.writeAll(node.key.?);

            const record = NodeRecord{
                .node_id = node.value.?.node_id,
                .value_type = node.value.?.value_type,
                .value_size = node.value.?.value_size,
                .value = node.key.?,
            };
            bloom_filter.add(&record);
            i += 1;
        }

        const index_start_offset: u32 = @intCast(try file.getPos());
        for (offsets.items) |offset| {
            try sstable.write(@TypeOf(offset), offset);
        }
        const index_end_offset: u32 = @intCast(try file.getPos());
        sstable.index_start_offset = index_start_offset;
        sstable.index_end_offset = index_end_offset;

        try sstable.write(u16, @as(u16, @intCast(min_key.len)));
        try file.writeAll(min_key);
        const min_key_buf = try allocator.alloc(u8, min_key.len);
        @memcpy(min_key_buf, min_key);
        sstable.min_value = min_key_buf;

        try sstable.write(u16, @as(u16, @intCast(max_key.len)));
        try file.writeAll(max_key);
        const max_key_buf = try allocator.alloc(u8, max_key.len);
        @memcpy(max_key_buf, max_key);
        sstable.max_value = max_key_buf;

        const bloom_start_offset: u32 = @intCast(try file.getPos());
        try file.writeAll(bloom_filter.filter);

        try sstable.write(u32, index_start_offset);
        try sstable.write(u32, index_end_offset);
        try sstable.write(u32, bloom_start_offset);

        return sstable;
    }

    pub fn open(path: []const u8, allocator: std.mem.Allocator) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        try file.seekFromEnd(-4);
        const bloom_start_offset = try read(u32, file);

        try file.seekFromEnd(-12);
        const bloom_end_offset: u32 = @intCast(try file.getPos());

        const bloom_size = bloom_end_offset - bloom_start_offset;
        const filter = try allocator.alloc(u8, bloom_size);

        try file.seekFromEnd(-12);
        const index_start_offset: u32 = try read(u32, file);
        try file.seekFromEnd(-8);
        const index_end_offset: u32 = try read(u32, file);
        try file.seekTo(index_end_offset);
        const min_key_size: u16 = try read(u16, file);
        const min_key_buf = try allocator.alloc(u8, min_key_size);
        _ = try file.read(min_key_buf[0..]);
        const max_key_size: u16 = try read(u16, file);
        const max_key_buf = try allocator.alloc(u8, max_key_size);
        _ = try file.read(max_key_buf[0..]);

        return SSTable{
            .path = path,
            .file = file,
            .bloom_filter = b.BloomFilter{
                .allocator = allocator,
                .filter = filter,
            },
            .allocator = allocator,
            .min_value = min_key_buf,
            .max_value = max_key_buf,
            .index_start_offset = index_start_offset,
            .index_end_offset = index_end_offset,
        };
    }

    pub inline fn close(self: *SSTable) void {
        if (self.min_value != null) {
            self.allocator.free(self.min_value.?);
        }
        if (self.max_value != null) {
            self.allocator.free(self.max_value.?);
        }
        if (self.bloom_filter != null) {
            self.bloom_filter.?.deinit();
        }
        self.file.close();
    }

    pub fn find(self: *const SSTable, key: []const u8) !?u64 {
        if (self.bloom_filter.?.may_contain(key) == false) return null;
        if (utils.compare_bitwise(key, self.min_value.?) < 0) return null;
        if (utils.compare_bitwise(key, self.max_value.?) > 0) return null;

        const search_result = try self.find_block_offset(key);
        if (search_result < 0) return null;

        const offset: u32 = @as(u32, @intCast(search_result));
        return try self.find_in_block(key, offset);
    }

    fn find_block_offset(self: *const SSTable, key: []const u8) !i64 {
        var low: u32 = 0;
        var high: u32 = ((self.index_end_offset - self.index_start_offset) / 4 - 1);
        while (low <= high) {
            const high_block_offset: u32 = try self.read_block_offset_from_index(high);
            const high_record = try self.read_record(high_block_offset);
            defer self.free_record(high_record);

            if (utils.compare_bitwise(key, high_record.value) > 0) {
                return @as(i64, @intCast(high_block_offset));
            }

            const mid = low + (high - low) / 2;
            const block_offset: u32 = try self.read_block_offset_from_index(mid);

            if (mid == low) return @as(i64, @intCast(block_offset));

            const record = try self.read_record(block_offset);
            defer self.free_record(record);

            if (utils.compare_bitwise(key, record.value) == 0) {
                return @as(i64, @intCast(block_offset));
            } else if (utils.compare_bitwise(key, record.value) < 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return -1;
    }

    fn find_in_block(self: *const SSTable, key: []const u8, block_offset: u32) !?u64 {
        try self.file.seekTo(@as(u64, @intCast(self.index_start_offset + 4)));
        const block_size = try read(u32, self.file);
        var offset = block_offset;
        while (offset < block_offset + block_size) {
            const record = try self.read_record(offset);
            defer self.free_record(record);

            if (utils.compare_bitwise(key, record.value) < 0) {
                return null;
            } else if (utils.compare_bitwise(key, record.value) == 0) {
                const node_id = record.node_id;
                return node_id;
            }
            offset = @as(u32, @intCast(try self.file.getPos()));
        }

        return null;
    }

    inline fn write(self: *const SSTable, comptime T: type, data: T) !void {
        var buffer: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buffer, data, std.builtin.Endian.big);
        try self.file.writeAll(&buffer);
    }

    inline fn read(comptime T: type, file: std.fs.File) !T {
        var buffer: [@sizeOf(T)]u8 = undefined;
        _ = try file.read(buffer[0..]);
        const value: T = std.mem.readInt(T, &buffer, std.builtin.Endian.big);
        return value;
    }

    fn read_block_offset_from_index(self: *const SSTable, index_offset: u32) !u32 {
        try self.file.seekTo(@intCast(self.index_start_offset + index_offset * 4));
        const block_offset: u32 = try read(u32, self.file);
        return block_offset;
    }

    fn read_record(self: *const SSTable, offset: u32) !*NodeRecord {
        try self.file.seekTo(@intCast(offset));
        const result = try self.allocator.create(NodeRecord);
        const node_id = try read(u64, self.file);
        const value_type: ValueType = @enumFromInt(try read(u8, self.file));
        const value_size: u16 = try read(u16, self.file);
        const record_value: []u8 = try self.allocator.alloc(u8, value_size);
        _ = try self.file.read(record_value[0..]);
        result.* = .{
            .node_id = node_id,
            .value_type = value_type,
            .value_size = value_size,
            .value = record_value,
        };
        return result;
    }

    fn free_record(self: *const SSTable, record: *NodeRecord) void {
        self.allocator.free(record.value);
        self.allocator.destroy(record);
    }
};

// Tests
const testing = std.testing;

const TEST_SSTABLE_PATH = "./test.sstable";

inline fn test_value() m.MemtableValue {
    return m.MemtableValue{
        .node_id = 0,
        .value_size = 8,
        .value_type = ValueType.bigserial,
    };
}

fn clean_up(comptime N: u8, storage: SSTable, memtable: m.Memtable(N)) !void {
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
    var test_memtable = try m.Memtable(8).init(testing.allocator, rng.random(), 0.125);
    defer test_memtable.destroy();

    const test_vertex_data = test_value();
    for (254..264) |i| {
        try test_memtable.add(&utils.key_from_int_data(usize, i), test_vertex_data);
    }

    var test_sstable = try SSTable.create(8, &test_memtable, TEST_SSTABLE_PATH, 76, testing.allocator);
    test_sstable.close();
    try clean_up(8, test_sstable, test_memtable);
}

test "SSTable#open" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = try m.Memtable(8).init(testing.allocator, rng.random(), 0.125);
    defer test_memtable.destroy();

    const test_vertex_data = test_value();
    for (254..264) |i| {
        try test_memtable.add(&utils.key_from_int_data(usize, i), test_vertex_data);
    }

    var test_sstable = try SSTable.create(8, &test_memtable, TEST_SSTABLE_PATH, 76, testing.allocator);
    test_sstable.close();

    test_sstable = try SSTable.open(TEST_SSTABLE_PATH, testing.allocator);

    try testing.expect(utils.compare_bitwise(test_sstable.min_value.?, &utils.key_from_int_data(usize, 254)) == 0);
    try testing.expect(utils.compare_bitwise(test_sstable.max_value.?, &utils.key_from_int_data(usize, 263)) == 0);

    test_sstable.close();
    try clean_up(8, test_sstable, test_memtable);
}

test "SSTable#find" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = try m.Memtable(8).init(testing.allocator, rng.random(), 0.125);
    defer test_memtable.destroy();

    const test_vertex_data = test_value();
    for (254..264) |i| {
        try test_memtable.add(&utils.key_from_int_data(usize, i), test_vertex_data);
    }

    var test_sstable = try SSTable.create(8, &test_memtable, TEST_SSTABLE_PATH, 76, testing.allocator);
    test_sstable.close();

    test_sstable = try SSTable.open(TEST_SSTABLE_PATH, testing.allocator);
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

    try clean_up(8, test_sstable, test_memtable);
}
