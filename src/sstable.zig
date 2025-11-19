const std = @import("std");
const FileWriter = std.fs.File.Writer;
const FileReader = std.fs.File.Reader;

const data_types = @import("./data_types.zig");
const Memtable = @import("./memtable.zig").Memtable;
const BloomFilter = @import("./bloom.zig").BloomFilter;
const utils = @import("./utils.zig");
const getConfigurator = @import("./global_context.zig").getConfigurator;

const StorageRecord = data_types.StorageRecord;

pub const MemtableIteratorAdapter = struct {
    memtable_iterator: Memtable.Iterator,

    pub fn init(memtable: *Memtable) MemtableIteratorAdapter {
        return .{ .memtable_iterator = memtable.iterator() };
    }

    fn nextFn(ctx: *anyopaque) !?StorageRecord {
        const self: *MemtableIteratorAdapter = @ptrCast(@alignCast(ctx));
        if (self.memtable_iterator.next()) |node| {
            return StorageRecord{
                .key = node.key.?,
                .value = node.value.?,
            };
        }
        return null;
    }

    pub fn iterator(self: *MemtableIteratorAdapter) StorageRecord.Iterator {
        return .{
            .context = self,
            .next_fn = nextFn,
        };
    }
};

pub const SSTableIteratorAdapter = struct {
    sstable_iterator: SSTable.Iterator,

    pub fn init(sstable: *SSTable) !SSTableIteratorAdapter {
        return .{ .sstable_iterator = try sstable.iterator() };
    }

    fn nextFn(ctx: *anyopaque) !?StorageRecord {
        const self: *SSTableIteratorAdapter = @ptrCast(@alignCast(ctx));
        if (try self.sstable_iterator.next()) |node| {
            return StorageRecord{
                .key = node.key,
                .value = node.value,
            };
        }
        return null;
    }

    pub fn iterator(self: *SSTableIteratorAdapter) StorageRecord.Iterator {
        return .{
            .context = self,
            .next_fn = nextFn,
        };
    }
};

pub const SSTable = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    records_number: u32,
    block_size: u32,
    bloom_filter: ?BloomFilter,
    index_start_offset: u32,
    bloom_start_offset: u32,
    min_key_start_offset: u32,
    max_key_start_offset: u32,
    min_key: ?[]const u8,
    max_key: ?[]const u8,
    index_buffer: ?[]const u8,
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

    pub const Iterator = struct {
        allocator: std.mem.Allocator,
        sstable: *const SSTable,
        reader: std.fs.File.Reader,
        reader_buffer: []u8,
        block_buffer: []u8,
        num_blocks: u32,
        current_block_num: u32,
        num_block_elements: u32,
        current_block_offset: u32,
        current_block_element_num: u32,

        pub fn init(allocator: std.mem.Allocator, sstable: *const SSTable) !Iterator {
            const block_buffer = try allocator.alloc(u8, sstable.block_size);
            const reader_buffer = try allocator.alloc(u8, 2);

            var reader = sstable.file.reader(reader_buffer);
            try reader.seekTo(0);
            _ = try reader.read(block_buffer);
            const num_elements: u32 = utils.intFromBytes(u32, block_buffer, block_buffer.len - 4);

            return Iterator{
                .allocator = allocator,
                .sstable = sstable,
                .reader = reader,
                .reader_buffer = reader_buffer,
                .block_buffer = block_buffer,
                .num_blocks = sstable.index_records_num,
                .current_block_num = 0,
                .num_block_elements = num_elements,
                .current_block_offset = 0,
                .current_block_element_num = 0,
            };
        }

        pub fn deinit(self: *Iterator) void {
            self.allocator.free(self.block_buffer);
            self.allocator.free(self.reader_buffer);
        }

        pub fn next(self: *Iterator) !?StorageRecord {
            if (self.current_block_num >= self.num_blocks) {
                return null;
            }

            if (self.current_block_element_num < self.num_block_elements) {
                return readRecordAndAdvance(self);
            } else {
                self.current_block_num += 1;
                if (self.current_block_num >= self.num_blocks) {
                    return null;
                }

                try self.reader.seekTo(self.current_block_num * self.sstable.block_size);
                _ = try self.reader.read(self.block_buffer);
                self.num_block_elements = utils.intFromBytes(u32, self.block_buffer, self.block_buffer.len - 4);

                self.current_block_offset = 0;
                self.current_block_element_num = 0;
                return readRecordAndAdvance(self);
            }
        }

        inline fn readRecordAndAdvance(self: *Iterator) StorageRecord {
            const record = SSTable.readRecordFromOffset(self.block_buffer, self.current_block_offset);
            self.current_block_offset += @as(u32, @intCast(record.key.len + record.value.len + 4));
            self.current_block_element_num += 1;
            return record;
        }
    };

    pub fn create(allocator: std.mem.Allocator, record_iterator: *StorageRecord.Iterator, records_number: u32, path: []const u8, block_size: u32, bits_per_key: u8) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });
        var writer = file.writer(&[0]u8{});

        var sstable = SSTable{
            .allocator = allocator,
            .path = path,
            .file = file,
            .records_number = records_number,
            .block_size = block_size,
            .bloom_filter = try BloomFilter.init(
                allocator,
                @intCast(records_number),
                bits_per_key,
            ),
            .index_start_offset = 0,
            .bloom_start_offset = 0,
            .min_key_start_offset = 0,
            .max_key_start_offset = 0,
            .min_key = null,
            .max_key = null,
            .index_buffer = null,
            .index_records_num = 0,
        };

        var index_records = try std.ArrayList(IndexRecord).initCapacity(allocator, 1);
        defer index_records.deinit(allocator);
        try sstable.writeDataBlocks(&writer, record_iterator, block_size, &index_records);
        var index_offsets: []u16 = try allocator.alloc(u16, index_records.items.len);
        defer allocator.free(index_offsets);

        sstable.index_start_offset = @intCast(writer.pos);
        var index_record_offset: u16 = 0;
        for (index_records.items, 0..) |record, i| {
            try utils.writeNumber(u16, &writer.interface, @as(u16, @intCast(record.min_key.len)));
            try writer.interface.writeAll(record.min_key);
            try utils.writeNumber(u32, &writer.interface, record.offset);
            index_offsets[i] = index_record_offset;
            index_record_offset += 6 + @as(u16, @intCast(record.min_key.len));
        }
        for (index_offsets) |index_offset| {
            try utils.writeNumber(u16, &writer.interface, index_offset);
        }
        sstable.index_records_num = @intCast(index_records.items.len);
        try utils.writeNumber(u32, &writer.interface, sstable.index_records_num);
        sstable.bloom_start_offset = @intCast(writer.pos);
        try writer.interface.writeAll(sstable.bloom_filter.?.filter);

        sstable.min_key_start_offset = @intCast(writer.pos);
        try utils.writeNumber(u16, &writer.interface, @as(u16, @intCast(sstable.min_key.?.len)));
        try writer.interface.writeAll(sstable.min_key.?);
        sstable.max_key_start_offset = @intCast(writer.pos);
        try utils.writeNumber(u16, &writer.interface, @as(u16, @intCast(sstable.max_key.?.len)));
        try writer.interface.writeAll(sstable.max_key.?);

        try utils.writeNumber(u32, &writer.interface, sstable.index_start_offset);
        try utils.writeNumber(u32, &writer.interface, sstable.bloom_start_offset);
        try utils.writeNumber(u32, &writer.interface, sstable.min_key_start_offset);
        try utils.writeNumber(u32, &writer.interface, sstable.max_key_start_offset);
        try utils.writeNumber(u32, &writer.interface, sstable.records_number);
        try utils.writeNumber(u32, &writer.interface, sstable.block_size);

        return sstable;
    }

    fn writeDataBlocks(self: *SSTable, writer: *FileWriter, record_iterator: *StorageRecord.Iterator, block_size: u32, index_records: *std.ArrayList(IndexRecord)) !void {
        var data_block = DataBlock{ .buffer = try self.allocator.alloc(u8, block_size) };
        defer self.allocator.free(data_block.buffer);

        var i: u16 = 0;
        var blocks_number: u32 = 0;
        var block_min_key: []const u8 = undefined;
        var block_offsets = try std.ArrayList(u16).initCapacity(self.allocator, 1);
        defer block_offsets.deinit(self.allocator);

        while (try record_iterator.next()) |record| : (i += 1) {
            if (i == 0) self.min_key = record.key;
            if (i == self.records_number - 1) self.max_key = record.key;

            self.bloom_filter.?.add(record.key);
            const record_size_with_offset: u32 = @as(u32, @intCast(record.key.len)) +
                @as(u32, @intCast(record.value.len)) +
                8 +
                (@as(u32, @intCast(block_offsets.items.len)) + 1) * 2;
            if (data_block.offset + record_size_with_offset > block_size) {
                try self.writeDataBlock(writer, &data_block, &block_offsets);
                try index_records.append(self.allocator, .{
                    .min_key = block_min_key,
                    .offset = self.block_size * blocks_number,
                });
                blocks_number += 1;
            }

            if (block_offsets.items.len == 0) {
                block_min_key = record.key;
            }
            try block_offsets.append(self.allocator, @intCast(data_block.offset));
            data_block.writeDataEntry(&record);
        }
        try self.writeDataBlock(writer, &data_block, &block_offsets);
        try index_records.append(self.allocator, .{
            .min_key = block_min_key,
            .offset = self.block_size * blocks_number,
        });
    }

    fn writeDataBlock(self: *SSTable, writer: *FileWriter, data_block: *DataBlock, block_offsets: *std.ArrayList(u16)) !void {
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

        try writer.interface.writeAll(data_block.buffer);
        @memset(data_block.buffer, 0);
        data_block.offset = 0;
        block_offsets.clearAndFree(self.allocator);
    }

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });
        const file_max_position = try file.getEndPos();
        var buffer: [4]u8 = [_]u8{0} ** 4;
        var reader = file.reader(&buffer);

        try reader.seekTo(file_max_position - 4);
        const block_size = try utils.readNumber(u32, &reader.interface);
        try reader.seekTo(file_max_position - 8);
        const records_number = try utils.readNumber(u32, &reader.interface);
        try reader.seekTo(file_max_position - 12);
        const max_key_start_offset = try utils.readNumber(u32, &reader.interface);

        try reader.seekTo(file_max_position - 16);
        const min_key_start_offset = try utils.readNumber(u32, &reader.interface);

        try reader.seekTo(file_max_position - 20);
        const bloom_start_offset = try utils.readNumber(u32, &reader.interface);
        const bloom_size: u32 = min_key_start_offset - bloom_start_offset;
        const filter = try allocator.alloc(u8, bloom_size);

        try reader.seekTo(file_max_position - 24);
        const index_start_offset: u32 = try utils.readNumber(u32, &reader.interface);

        try reader.seekTo(@intCast(bloom_start_offset - 4));
        const index_records_num: u32 = try utils.readNumber(u32, &reader.interface);

        try reader.seekTo(bloom_start_offset);
        _ = try reader.read(filter);

        const min_key_buf = try readKeyFromOffset(allocator, &reader, min_key_start_offset);
        const max_key_buf = try readKeyFromOffset(allocator, &reader, max_key_start_offset);
        const index = try allocator.alloc(u8, bloom_start_offset - index_start_offset);
        try reader.seekTo(index_start_offset);
        _ = try reader.read(index);

        return SSTable{
            .allocator = allocator,
            .path = path,
            .file = file,
            .records_number = records_number,
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
            .index_buffer = index,
            .index_records_num = index_records_num,
        };
    }

    pub inline fn close(self: *const SSTable, free_min_max_keys: bool) void {
        if (free_min_max_keys) {
            if (self.min_key) |min_key| {
                self.allocator.free(min_key);
            }
            if (self.max_key) |max_key| {
                self.allocator.free(max_key);
            }
        }
        if (self.bloom_filter) |bloom_filter| {
            bloom_filter.deinit();
        }
        if (self.index_buffer) |index_buffer| {
            self.allocator.free(index_buffer);
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

    pub inline fn readRecordFromOffset(buffer: []const u8, offset: u32) StorageRecord {
        const key_size: u16 = utils.intFromBytes(u16, buffer, offset);
        const value_size: u16 = utils.intFromBytes(u16, buffer, offset + 2 + key_size);
        return .{
            .key = buffer[offset + 2 .. offset + 2 + key_size],
            .value = buffer[offset + key_size + 4 .. offset + key_size + 4 + value_size],
        };
    }

    pub fn iterator(self: *SSTable) !Iterator {
        return try Iterator.init(self.allocator, self);
    }

    fn findBlockOffset(self: *const SSTable, key: data_types.BinaryData) !i64 {
        var low: u32 = 0;
        var high: u32 = self.index_records_num - 1;
        while (low <= high) {
            const high_index_record_offset: u32 = try self.readIndexRecordOffset(high);
            const high_index_record_min_key = readKeyFromIndexBuffer(self.index_buffer.?, high_index_record_offset);

            if (utils.compareBitwise(key, high_index_record_min_key) >= 0) {
                const data_block_offset: u32 = utils.intFromBytes(
                    u32,
                    self.index_buffer.?,
                    high_index_record_offset + 2 + high_index_record_min_key.len,
                );
                return @intCast(data_block_offset);
            }

            const mid = low + (high - low) / 2;
            const index_record_offset: u32 = try self.readIndexRecordOffset(mid);
            if (mid == low) {
                const key_size: u16 = utils.intFromBytes(u16, self.index_buffer.?, index_record_offset);
                const data_block_offset: u32 = utils.intFromBytes(
                    u32,
                    self.index_buffer.?,
                    index_record_offset + 2 + key_size,
                );
                return @intCast(data_block_offset);
            }

            const index_record_min_key = readKeyFromIndexBuffer(self.index_buffer.?, index_record_offset);
            const compare_result = utils.compareBitwise(key, index_record_min_key);
            if (compare_result == 0) {
                const data_block_offset: u32 = utils.intFromBytes(
                    u32,
                    self.index_buffer.?,
                    index_record_offset + 2 + index_record_min_key.len,
                );
                return @intCast(data_block_offset);
            } else if (compare_result < 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return -1;
    }

    fn findInBlock(self: *const SSTable, key: data_types.BinaryData, block_offset: u32) !?data_types.BinaryData {
        const block_buffer: []u8 = try self.allocator.alloc(u8, self.block_size);
        defer self.allocator.free(block_buffer);

        const reader_buffer: []u8 = try self.allocator.alloc(u8, 2);
        defer self.allocator.free(reader_buffer);

        var reader = self.file.reader(reader_buffer);
        try reader.seekTo(block_offset);
        _ = try reader.read(block_buffer);

        const num_elements: u32 = utils.intFromBytes(u32, block_buffer, block_buffer.len - 4);
        var low: u32 = 0;
        var high: u32 = num_elements - 1;
        while (low <= high) {
            const mid = low + (high - low) / 2;
            const record_offset: u16 = utils.intFromBytes(
                u16,
                block_buffer,
                block_buffer.len - 4 - 2 * (num_elements - mid),
            );
            const record = readRecordFromOffset(block_buffer, record_offset);
            const compare_result = utils.compareBitwise(key, record.key);
            if (compare_result == 0) {
                const result = try self.allocator.alloc(u8, record.value.len);
                @memcpy(result, record.value);
                return result;
            } else if (compare_result < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return null;
    }

    inline fn readKeyFromOffset(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, offset: u32) ![]const u8 {
        reader.pos = offset;
        const key_size: u16 = try utils.readNumber(u16, &reader.interface);
        reader.pos = offset + 2;
        const key: []u8 = try allocator.alloc(u8, key_size);
        _ = try reader.read(key);
        return key;
    }

    inline fn readKeyFromIndexBuffer(index_buffer: []const u8, offset: u32) []const u8 {
        const key_size: u16 = utils.intFromBytes(u16, index_buffer, offset);
        return index_buffer[offset + 2 .. offset + 2 + key_size];
    }

    inline fn readIndexRecordOffset(self: *const SSTable, record_number: u32) !u32 {
        return utils.intFromBytes(u16, self.index_buffer.?, self.index_buffer.?.len - 4 - 2 * (self.index_records_num - record_number));
    }
};

// Tests
const testing = std.testing;

const TEST_SSTABLE_PATH = "./test.sstable";

fn cleanup(storage: SSTable, memtable: Memtable) !void {
    try std.fs.cwd().deleteFile(storage.path);
    try memtable.wal.deleteFile();
}

test "SSTable#create" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (254..264) |i| {
        slot = test_memtable.reserve(@sizeOf(usize) + test_value.len);
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value, &slot.?);
    }

    var adapter = MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(testing.allocator, &memtable_iterator, test_memtable.size, TEST_SSTABLE_PATH, 52, 20);
    try testing.expect(test_sstable.index_start_offset == 0xd0);
    try testing.expect(test_sstable.bloom_start_offset == 0x114);
    try testing.expect(test_sstable.min_key_start_offset == 0x12e);
    try testing.expect(test_sstable.max_key_start_offset == 0x138);
    try testing.expect(test_sstable.records_number == 10);
    try testing.expect(test_sstable.block_size == 52);
    try testing.expect(test_sstable.index_records_num == 4);

    test_sstable.close(false);
    try cleanup(test_sstable, test_memtable);
}

test "SSTable#open" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (254..264) |i| {
        slot = test_memtable.reserve(@sizeOf(usize) + test_value.len);
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value, &slot.?);
    }

    var adapter = MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(testing.allocator, &memtable_iterator, test_memtable.size, TEST_SSTABLE_PATH, 52, 20);
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, TEST_SSTABLE_PATH);

    try testing.expect(std.mem.eql(u8, test_sstable.min_key.?, &utils.intToBytes(usize, 254)));
    try testing.expect(std.mem.eql(u8, test_sstable.max_key.?, &utils.intToBytes(usize, 263)));
    try testing.expect(test_sstable.index_start_offset == 0xd0);
    try testing.expect(test_sstable.bloom_start_offset == 0x114);
    try testing.expect(test_sstable.min_key_start_offset == 0x12e);
    try testing.expect(test_sstable.max_key_start_offset == 0x138);
    try testing.expect(test_sstable.records_number == 10);
    try testing.expect(test_sstable.block_size == 52);
    try testing.expect(test_sstable.index_records_num == 4);

    test_sstable.close(true);
    try cleanup(test_sstable, test_memtable);
}

test "SSTable#find" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (254..264) |i| {
        slot = test_memtable.reserve(@sizeOf(usize) + test_value.len);
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value, &slot.?);
    }

    var adapter = MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(testing.allocator, &memtable_iterator, test_memtable.size, TEST_SSTABLE_PATH, 52, 20);
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, TEST_SSTABLE_PATH);
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

test "SSTable#iterator" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (254..264) |i| {
        slot = test_memtable.reserve(@sizeOf(usize) + test_value.len);
        try test_memtable.add(&utils.intToBytes(usize, i), &test_value, &slot.?);
    }

    var adapter = MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(testing.allocator, &memtable_iterator, test_memtable.size, TEST_SSTABLE_PATH, 52, 20);
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, TEST_SSTABLE_PATH);
    defer test_sstable.close(true);

    var iterator = try test_sstable.iterator();
    defer iterator.deinit();

    var expected_key: usize = 254;
    while (try iterator.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key, &utils.intToBytes(usize, expected_key)));
        try testing.expect(std.mem.eql(u8, record.value, &utils.intToBytes(u8, 0)));
        expected_key += 1;
    }
    try testing.expect(expected_key == 264);

    try cleanup(test_sstable, test_memtable);
}
