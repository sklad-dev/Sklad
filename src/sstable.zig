const std = @import("std");
const data_types = @import("./data_types.zig");
const Memtable = @import("./memtable.zig").Memtable;
const BloomFilter = @import("./bloom.zig").BloomFilter;
const utils = @import("./utils.zig");
const getConfigurator = @import("./global_context.zig").getConfigurator;

const StorageRecord = data_types.StorageRecord;

pub const SSTable = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    block_size: u32,
    bloom_filter: ?BloomFilter,
    index_start_offset: u32,
    bloom_start_offset: u32,
    min_key_start_offset: u32,
    max_key_start_offset: u32,
    min_key: ?[]const u8,
    max_key: ?[]const u8,
    index_records_num: u32,

    pub const DataBlock = struct {
        buffer: []u8,
        offset: u32 = 0,

        pub fn writeDataEntry(self: *DataBlock, record: *const StorageRecord) void {
            @memcpy(self.buffer[self.offset .. self.offset + 2], &utils.intToBytes(u16, @intCast(record.key.len)));
            self.offset += 2;
            @memcpy(self.buffer[self.offset..(self.offset + @as(u32, @intCast(record.key.len)))], record.key);
            self.offset += @intCast(record.key.len);
            @memcpy(self.buffer[self.offset .. self.offset + 2], &utils.intToBytes(u16, @intCast(record.value.len)));
            self.offset += 2;
            @memcpy(self.buffer[self.offset..(self.offset + @as(u32, @intCast(record.value.len)))], record.value);
            self.offset += @intCast(record.value.len);
        }
    };

    const IndexRecord = struct {
        min_key: []const u8,
        offset: u32,
    };

    pub fn create(allocator: std.mem.Allocator, memtable: *Memtable, path: []const u8, block_size: u32, bits_per_key: u8) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        var sstable = SSTable{
            .allocator = allocator,
            .path = path,
            .file = file,
            .block_size = block_size,
            .bloom_filter = try BloomFilter.init(
                allocator,
                @intCast(memtable.size),
                bits_per_key,
            ),
            .index_start_offset = 0,
            .bloom_start_offset = 0,
            .min_key_start_offset = 0,
            .max_key_start_offset = 0,
            .min_key = null,
            .max_key = null,
            .index_records_num = 0,
        };

        var index_records = std.ArrayList(IndexRecord).init(allocator);
        defer index_records.deinit();
        try sstable.writeDataBlocks(memtable, block_size, &index_records);
        var index_offsets: []u16 = try allocator.alloc(u16, index_records.items.len);
        defer allocator.free(index_offsets);

        sstable.index_start_offset = @intCast(try file.getPos());
        var index_record_offset: u16 = 0;
        for (index_records.items, 0..) |record, i| {
            try utils.writeNumber(u16, sstable.file, @as(u16, @intCast(record.min_key.len)));
            try file.writeAll(record.min_key);
            try utils.writeNumber(u32, sstable.file, record.offset);
            index_offsets[i] = index_record_offset;
            index_record_offset += 6 + @as(u16, @intCast(record.min_key.len));
        }
        for (index_offsets) |index_offset| {
            try utils.writeNumber(u16, sstable.file, index_offset);
        }
        sstable.index_records_num = @intCast(index_records.items.len);
        try utils.writeNumber(u32, sstable.file, sstable.index_records_num);
        sstable.bloom_start_offset = @intCast(try file.getPos());
        try file.writeAll(sstable.bloom_filter.?.filter);

        sstable.min_key_start_offset = @intCast(try file.getPos());
        try utils.writeNumber(u16, sstable.file, @as(u16, @intCast(sstable.min_key.?.len)));
        try file.writeAll(sstable.min_key.?);
        sstable.max_key_start_offset = @intCast(try file.getPos());
        try utils.writeNumber(u16, sstable.file, @as(u16, @intCast(sstable.max_key.?.len)));
        try file.writeAll(sstable.max_key.?);

        try utils.writeNumber(u32, sstable.file, sstable.index_start_offset);
        try utils.writeNumber(u32, sstable.file, sstable.bloom_start_offset);
        try utils.writeNumber(u32, sstable.file, sstable.min_key_start_offset);
        try utils.writeNumber(u32, sstable.file, sstable.max_key_start_offset);
        try utils.writeNumber(u32, sstable.file, sstable.block_size);

        return sstable;
    }

    fn writeDataBlocks(self: *SSTable, memtable: *Memtable, block_size: u32, index_records: *std.ArrayList(IndexRecord)) !void {
        var data_block = DataBlock{ .buffer = try self.allocator.alloc(u8, block_size) };
        defer self.allocator.free(data_block.buffer);

        var i: u16 = 0;
        var blocks_number: u32 = 0;
        var block_min_key: []const u8 = undefined;
        var block_offsets = std.ArrayList(u16).init(self.allocator);
        defer block_offsets.deinit();

        var iterator = memtable.iterator();
        while (iterator.next()) |node| : (i += 1) {
            const record = StorageRecord{
                .key = node.key.?,
                .value = node.value.?,
            };
            if (i == 0) self.min_key = record.key;
            if (i == memtable.size - 1) self.max_key = record.key;

            self.bloom_filter.?.add(record.key);
            const record_size_with_offset: u32 = @as(u32, @intCast(record.key.len)) +
                @as(u32, @intCast(record.value.len)) +
                8 +
                (@as(u32, @intCast(block_offsets.items.len)) + 1) * 2;
            if (data_block.offset + record_size_with_offset > block_size) {
                try self.writeDataBLock(&data_block, &block_offsets);
                try index_records.append(.{
                    .min_key = block_min_key,
                    .offset = self.block_size * blocks_number,
                });
                blocks_number += 1;
            }

            if (block_offsets.items.len == 0) {
                block_min_key = node.key.?;
            }
            try block_offsets.append(@intCast(data_block.offset));
            data_block.writeDataEntry(&record);
        }
        try self.writeDataBLock(&data_block, &block_offsets);
        try index_records.append(.{
            .min_key = block_min_key,
            .offset = self.block_size * blocks_number,
        });
    }

    fn writeDataBLock(self: *SSTable, data_block: *DataBlock, block_offsets: *std.ArrayList(u16)) !void {
        const num_offsets = block_offsets.items.len;
        const buffer_len = data_block.buffer.len;
        @memcpy(data_block.buffer[buffer_len - 4 ..], &utils.intToBytes(u32, @intCast(block_offsets.items.len)));
        for (block_offsets.items, 0..) |offset, j| {
            const start_offset = buffer_len - 4 - (num_offsets - j) * 2;
            @memcpy(data_block.buffer[start_offset .. start_offset + 2], &utils.intToBytes(u16, offset));
        }
        if (data_block.offset < buffer_len - 4 - num_offsets * 2) {
            @memset(data_block.buffer[data_block.offset..(buffer_len - 4 - num_offsets * 2)], 0);
        }

        try self.file.writeAll(data_block.buffer);
        @memset(data_block.buffer, 0);
        data_block.offset = 0;
        block_offsets.clearAndFree();
    }

    pub fn open(path: []const u8, allocator: std.mem.Allocator) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        try file.seekFromEnd(-4);
        const block_size = try utils.readNumber(u32, file);

        try file.seekFromEnd(-8);
        const max_key_start_offset = try utils.readNumber(u32, file);

        try file.seekFromEnd(-12);
        const min_key_start_offset = try utils.readNumber(u32, file);

        try file.seekFromEnd(-16);
        const bloom_start_offset = try utils.readNumber(u32, file);

        try file.seekFromEnd(-20);
        const bloom_size: u32 = @as(u32, @intCast(try file.getPos())) - bloom_start_offset;
        const index_start_offset: u32 = try utils.readNumber(u32, file);

        const filter = try allocator.alloc(u8, bloom_size);
        _ = try file.readAll(filter);

        try file.seekTo(@intCast(bloom_start_offset - 4));
        const index_records_num: u32 = try utils.readNumber(u32, file);

        const min_key_buf = try readKeyFromOffset(allocator, file, min_key_start_offset);
        const max_key_buf = try readKeyFromOffset(allocator, file, max_key_start_offset);

        return SSTable{
            .allocator = allocator,
            .path = path,
            .file = file,
            .block_size = block_size,
            .bloom_filter = BloomFilter{
                .allocator = allocator,
                .filter = filter,
            },
            .index_start_offset = index_start_offset,
            .bloom_start_offset = bloom_start_offset,
            .min_key_start_offset = min_key_start_offset,
            .max_key_start_offset = max_key_start_offset,
            .min_key = min_key_buf,
            .max_key = max_key_buf,
            .index_records_num = index_records_num,
        };
    }

    pub inline fn close(self: *const SSTable, free_min_max_keys: bool) void {
        if (free_min_max_keys) {
            if (self.min_key != null) {
                self.allocator.free(self.min_key.?);
            }
            if (self.max_key != null) {
                self.allocator.free(self.max_key.?);
            }
        }
        if (self.bloom_filter != null) {
            self.bloom_filter.?.deinit();
        }
        self.file.close();
    }

    pub fn find(self: *const SSTable, key: data_types.BinaryData) !?data_types.BinaryData {
        if (self.bloom_filter.?.mayContain(key) == false) return null;
        if (utils.compareBitwise(key, self.min_key.?) < 0) return null;
        if (utils.compareBitwise(key, self.max_key.?) > 0) return null;

        const search_result = try self.findBlockOffset(key);
        if (search_result < 0) return null;

        const offset: u32 = @as(u32, @intCast(search_result));
        return try self.findInBlock(key, offset);
    }

    fn findBlockOffset(self: *const SSTable, key: data_types.BinaryData) !i64 {
        var low: u32 = 0;
        var high: u32 = self.index_records_num - 1;
        while (low <= high) {
            const high_index_record_offset: u32 = try self.readIndexRecordOffset(high);
            const high_index_record_min_key = try readKeyFromOffset(
                self.allocator,
                self.file,
                high_index_record_offset,
            );
            defer self.allocator.free(high_index_record_min_key);

            if (utils.compareBitwise(key, high_index_record_min_key) >= 0) {
                try self.file.seekTo(@intCast(high_index_record_offset + 2 + high_index_record_min_key.len));
                const data_block_offset: u32 = try utils.readNumber(u32, self.file);
                return @intCast(data_block_offset);
            }

            const mid = low + (high - low) / 2;
            const index_record_offset: u32 = try self.readIndexRecordOffset(mid);
            if (mid == low) {
                try self.file.seekTo(@intCast(index_record_offset));
                const key_size: u16 = try utils.readNumber(u16, self.file);
                try self.file.seekTo(@as(u64, @intCast(index_record_offset)) + @as(u64, @intCast(key_size)) + 2);
                const data_block_offset: u32 = try utils.readNumber(u32, self.file);
                return @intCast(data_block_offset);
            }

            const index_record_min_key = try readKeyFromOffset(
                self.allocator,
                self.file,
                index_record_offset,
            );
            defer self.allocator.free(index_record_min_key);

            if (utils.compareBitwise(key, index_record_min_key) == 0) {
                try self.file.seekTo(@intCast(index_record_offset + 2 + index_record_min_key.len));
                const data_block_offset: u32 = try utils.readNumber(u32, self.file);
                return @intCast(data_block_offset);
            } else if (utils.compareBitwise(key, index_record_min_key) < 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return -1;
    }

    fn findInBlock(self: *const SSTable, key: data_types.BinaryData, block_offset: u32) !?data_types.BinaryData {
        var offset = block_offset;
        while (offset < block_offset + self.block_size) {
            const record = try StorageRecord.readFromOffset(self.allocator, self.file, offset);
            defer record.destroy(self.allocator);

            if (utils.compareBitwise(key, record.key) < 0) {
                return null;
            } else if (utils.compareBitwise(key, record.key) == 0) {
                const result = try self.allocator.alloc(u8, record.value.len);
                @memcpy(result, record.value);
                return result;
            }
            offset = @as(u32, @intCast(try self.file.getPos()));
        }

        return null;
    }

    inline fn readKeyFromOffset(allocator: std.mem.Allocator, file: std.fs.File, offset: u32) ![]const u8 {
        try file.seekTo(@intCast(offset));
        const key_size: u16 = try utils.readNumber(u16, file);
        const key: []u8 = try allocator.alloc(u8, key_size);
        _ = try file.read(key[0..]);
        return key;
    }

    inline fn readIndexRecordOffset(self: *const SSTable, record_number: u32) !u32 {
        try self.file.seekTo(@intCast(self.bloom_start_offset - 4 - 2 * (self.index_records_num - record_number)));
        const record_offset: u16 = try utils.readNumber(u16, self.file);
        return self.index_start_offset + @as(u32, @intCast(record_offset));
    }
};

// Tests
const testing = std.testing;

const TEST_SSTABLE_PATH = "./test.sstable";

fn cleanup(storage: SSTable, memtable: Memtable) !void {
    std.fs.cwd().deleteFile(storage.path) catch {
        const out = std.io.getStdOut().writer();
        try std.fmt.format(out, "failed to clean up after the test\n", .{});
    };
    try memtable.wal.deleteFile();
}

test "SSTable#create" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    for (254..264) |i| {
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value);
    }

    var test_sstable = try SSTable.create(testing.allocator, &test_memtable, TEST_SSTABLE_PATH, 52, 20);
    try testing.expect(test_sstable.index_start_offset == 0xd0);
    try testing.expect(test_sstable.bloom_start_offset == 0x114);
    try testing.expect(test_sstable.min_key_start_offset == 0x12e);
    try testing.expect(test_sstable.max_key_start_offset == 0x138);
    try testing.expect(test_sstable.block_size == 52);
    try testing.expect(test_sstable.index_records_num == 4);

    test_sstable.close(false);
    try cleanup(test_sstable, test_memtable);
}

test "SSTable#open" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    for (254..264) |i| {
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value);
    }

    var test_sstable = try SSTable.create(testing.allocator, &test_memtable, TEST_SSTABLE_PATH, 52, 20);
    test_sstable.close(false);

    test_sstable = try SSTable.open(TEST_SSTABLE_PATH, testing.allocator);

    try testing.expect(std.mem.eql(u8, test_sstable.min_key.?, &utils.intToBytes(usize, 254)));
    try testing.expect(std.mem.eql(u8, test_sstable.max_key.?, &utils.intToBytes(usize, 263)));
    try testing.expect(test_sstable.index_start_offset == 0xd0);
    try testing.expect(test_sstable.bloom_start_offset == 0x114);
    try testing.expect(test_sstable.min_key_start_offset == 0x12e);
    try testing.expect(test_sstable.max_key_start_offset == 0x138);
    try testing.expect(test_sstable.block_size == 52);
    try testing.expect(test_sstable.index_records_num == 4);

    test_sstable.close(true);
    try cleanup(test_sstable, test_memtable);
}

test "SSTable#find" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    for (254..264) |i| {
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value);
    }

    var test_sstable = try SSTable.create(testing.allocator, &test_memtable, TEST_SSTABLE_PATH, 52, 20);
    test_sstable.close(false);

    test_sstable = try SSTable.open(TEST_SSTABLE_PATH, testing.allocator);
    defer test_sstable.close(true);

    const vs = [7]usize{ 254, 256, 257, 258, 259, 261, 263 };
    for (vs) |v| {
        const result = try test_sstable.find(&utils.intToBytes(usize, v));
        defer {
            if (result) |r| testing.allocator.free(r);
        }
        try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u8, 0)));
    }

    const nvs = [2]usize{ 200, 300 };
    for (nvs) |v| {
        try testing.expect(try test_sstable.find(&utils.intToBytes(usize, v)) == null);
    }

    try cleanup(test_sstable, test_memtable);
}
