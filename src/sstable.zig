const std = @import("std");
const FileWriter = std.fs.File.Writer;
const FileReader = std.fs.File.Reader;

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const Memtable = @import("./memtable.zig").Memtable;
const BloomFilter = @import("./bloom.zig").BloomFilter;
const utils = @import("./utils.zig");
const getWorkerContext = @import("./worker.zig").getWorkerContext;

const FileHandle = data_types.FileHandle;
const BinaryData = data_types.BinaryData;
const BinaryDataRange = data_types.BinaryDataRange;
const StorageRecord = data_types.StorageRecord;
const RecordKey = data_types.RecordKey;
const RecordValue = data_types.RecordValue;

pub const SSTableIteratorAdapter = struct {
    sstable_iterator: SSTable.Iterator,

    pub fn init(sstable: *SSTable) !SSTableIteratorAdapter {
        return .{ .sstable_iterator = try sstable.iterator() };
    }

    fn nextFn(ctx: *anyopaque) !?StorageRecord {
        const self: *SSTableIteratorAdapter = @ptrCast(@alignCast(ctx));
        return try self.sstable_iterator.next();
    }

    pub fn iterator(self: *SSTableIteratorAdapter) StorageRecord.Iterator {
        return .{
            .context = self,
            .next_fn = nextFn,
        };
    }
};

pub inline fn fileNameFromHandle(allocator: std.mem.Allocator, path: []const u8, handle: FileHandle) ![]u8 {
    const buf_size = 10 + path.len + utils.numDigits(u8, handle.level) + utils.numDigits(u64, handle.file_id);
    const buf = try allocator.alloc(u8, buf_size);
    const file_name = try std.fmt.bufPrint(
        buf,
        "{s}/{d}.{d}.sstable",
        .{ path, handle.level, handle.file_id },
    );

    return file_name;
}

pub const SSTable = struct {
    allocator: std.mem.Allocator,
    handle: FileHandle,
    file: std.fs.File,
    records_number: u32,
    block_size: u32,
    bloom_filter: ?BloomFilter,
    index_start_offset: u64,
    bloom_start_offset: u64,
    min_key_start_offset: u64,
    max_key_start_offset: u64,
    min_key: ?[]const u8,
    max_key: ?[]const u8,
    index_buffer: ?[]const u8,
    index_records_num: u32,

    pub const DataBlock = struct {
        buffer: []u8,
        offset: u32 = 0,

        pub fn writeDataEntry(self: *DataBlock, record: *const StorageRecord) void {
            record.writeToBuffer(self.buffer, self.offset);
            self.offset += @intCast(record.sizeInMemory());
        }
    };

    pub const Iterator = struct {
        allocator: std.mem.Allocator,
        sstable: *const SSTable,
        block_buffer: []u8,
        num_blocks: u32,
        current_block_num: u32,
        num_block_elements: u32,
        current_block_offset: u32,
        current_block_element_num: u32,
        key_range: ?BinaryDataRange,

        const RecordPosition = struct {
            element_index: u32,
            record_offset: u32,
        };

        pub fn init(allocator: std.mem.Allocator, sstable: *const SSTable) !Iterator {
            const block_buffer = try allocator.alloc(u8, sstable.block_size);
            _ = try sstable.file.pread(block_buffer, 0);
            const num_elements: u32 = utils.intFromBytes(u32, block_buffer, block_buffer.len - 4);

            return Iterator{
                .allocator = allocator,
                .sstable = sstable,
                .block_buffer = block_buffer,
                .num_blocks = sstable.index_records_num,
                .current_block_num = 0,
                .num_block_elements = num_elements,
                .current_block_offset = 0,
                .current_block_element_num = 0,
                .key_range = null,
            };
        }

        pub fn deinit(self: *Iterator) void {
            self.allocator.free(self.block_buffer);
        }

        pub fn setRange(self: *Iterator, range: BinaryDataRange) !void {
            self.key_range = range;
            _ = try self.seekTo(range.start);
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

                _ = try self.sstable.file.pread(
                    self.block_buffer,
                    @as(u64, @intCast(self.current_block_num)) * @as(u64, @intCast(self.sstable.block_size)),
                );
                self.num_block_elements = utils.intFromBytes(u32, self.block_buffer, self.block_buffer.len - 4);

                self.current_block_offset = 0;
                self.current_block_element_num = 0;
                return readRecordAndAdvance(self);
            }
        }

        fn seekTo(self: *Iterator, key: BinaryData) !bool {
            const block_offset = try self.sstable.findBlockOffset(key) orelse return false;
            self.current_block_num = @intCast(block_offset / @as(u64, @intCast(self.sstable.block_size)));
            _ = try self.sstable.file.pread(self.block_buffer, block_offset);
            self.num_block_elements = utils.intFromBytes(u32, self.block_buffer, self.block_buffer.len - 4);
            self.current_block_offset = 0;
            self.current_block_element_num = 0;

            if (seekToKeyOffset(self.block_buffer, self.num_block_elements, key)) |pos| {
                self.current_block_offset = pos.record_offset;
                self.current_block_element_num = pos.element_index;
                return true;
            }

            return false;
        }

        fn seekToKeyOffset(block_buffer: []u8, num_elements: u32, key: BinaryData) ?RecordPosition {
            if (num_elements == 0) return null;

            var low: u32 = 0;
            var high: u32 = num_elements;

            while (low < high) {
                const mid = low + (high - low) / 2;
                const record_offset: u32 = utils.intFromBytes(
                    u32,
                    block_buffer,
                    block_buffer.len - 4 - 4 * (num_elements - mid),
                );
                const record = StorageRecord.fromBytes(block_buffer, record_offset);
                const compare_result = utils.compareBitwise(record.key.data, key);

                if (compare_result < 0) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            if (low >= num_elements) {
                return null;
            }

            const record_offset: u32 = utils.intFromBytes(
                u32,
                block_buffer,
                block_buffer.len - 4 - 4 * (num_elements - low),
            );
            return .{ .element_index = low, .record_offset = record_offset };
        }

        inline fn readRecordAndAdvance(self: *Iterator) ?StorageRecord {
            const record = StorageRecord.fromBytes(self.block_buffer, self.current_block_offset);
            self.current_block_offset += @as(u32, @intCast(record.sizeInMemory()));
            self.current_block_element_num += 1;
            if (self.key_range) |range| {
                if (utils.compareBitwise(record.key.data, range.end) > 0) {
                    return null;
                }
            }
            return record;
        }
    };

    pub fn create(
        allocator: std.mem.Allocator,
        record_iterator: *StorageRecord.Iterator,
        max_records_number: u32,
        handle: FileHandle,
        block_size: u32,
        bits_per_key: u8,
    ) !SSTable {
        const path = try fileNameFromHandle(allocator, global_context.getRootFolder(), handle);
        defer allocator.free(path);

        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });
        var writer = file.writer(&[0]u8{});

        var sstable = SSTable{
            .allocator = allocator,
            .handle = handle,
            .file = file,
            .records_number = 0,
            .block_size = block_size,
            .bloom_filter = try BloomFilter.init(
                allocator,
                @intCast(max_records_number),
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

        const blocks_number: u32 = try sstable.writeDataBlocks(&writer, record_iterator, block_size);
        try sstable.writeIndexBlock(&writer, blocks_number);
        try sstable.writeBloom(&writer);
        try sstable.writeMinMaxKeys(&writer);
        try sstable.writeFooter(&writer);

        try sstable.file.sync();

        return sstable;
    }

    fn writeDataBlocks(self: *SSTable, writer: *FileWriter, record_iterator: *StorageRecord.Iterator, block_size: u32) !u32 {
        var data_block = DataBlock{ .buffer = getWorkerContext().?.block_buffer };

        var i: u32 = 0;
        var blocks_number: u32 = 0;
        var block_min_key: []const u8 = undefined;
        var block_offsets = try std.ArrayList(u32).initCapacity(self.allocator, 1);
        defer block_offsets.deinit(self.allocator);

        var written_records: u32 = 0;
        while (try record_iterator.next()) |record| : (i += 1) {
            if (record.isExpired()) continue;

            written_records += 1;
            self.max_key = record.key.data;

            self.bloom_filter.?.add(record.key.data);
            const record_size_with_offset: u32 = @as(u32, @intCast(record.sizeInMemory())) +
                4 +
                (@as(u32, @intCast(block_offsets.items.len)) + 1) * 4;
            if (data_block.offset + record_size_with_offset > block_size) {
                try writeDataBlock(writer, &data_block, &block_offsets);
                blocks_number += 1;
            }

            if (block_offsets.items.len == 0) {
                block_min_key = record.key.data;
            }
            try block_offsets.append(self.allocator, @intCast(data_block.offset));
            data_block.writeDataEntry(&record);
        }
        if (block_offsets.items.len > 0) {
            blocks_number += 1;
            try writeDataBlock(writer, &data_block, &block_offsets);
        }
        self.records_number = written_records;

        return blocks_number;
    }

    fn writeDataBlock(writer: *FileWriter, data_block: *DataBlock, block_offsets: *std.ArrayList(u32)) !void {
        const num_offsets = block_offsets.items.len;
        const buffer_len = data_block.buffer.len;
        @memcpy(data_block.buffer[buffer_len - 4 ..], &utils.intToBytes(u32, @intCast(block_offsets.items.len)));
        for (block_offsets.items, 0..) |offset, j| {
            const start_offset = buffer_len - 4 - (num_offsets - j) * 4;
            @memcpy(data_block.buffer[start_offset .. start_offset + 4], &utils.intToBytes(u32, offset));
        }
        if (data_block.offset < buffer_len - 4 - num_offsets * 4) {
            @memset(data_block.buffer[data_block.offset..(buffer_len - 4 - num_offsets * 4)], 0);
        }

        try writer.interface.writeAll(data_block.buffer);
        @memset(data_block.buffer, 0);
        data_block.offset = 0;
        block_offsets.clearRetainingCapacity();
    }

    inline fn writeIndexBlock(self: *SSTable, writer: *FileWriter, blocks_number: u32) !void {
        self.index_start_offset = @intCast(writer.pos);

        var index_offsets: []u32 = try self.allocator.alloc(u32, blocks_number);
        defer self.allocator.free(index_offsets);

        var index_record_offset: u32 = 0;
        var block_buffer = getWorkerContext().?.block_buffer;
        var block_offset: u64 = 0;
        var reader = self.file.reader(getWorkerContext().?.reader_buffer[0..4]);

        for (0..blocks_number) |i| {
            block_offset = self.block_size * i;
            try reader.seekTo(block_offset);
            _ = try reader.interface.readSliceAll(block_buffer);

            const key_size = utils.intFromBytes(u16, block_buffer, 0);
            const key = block_buffer[2 .. 2 + key_size];

            try utils.writeNumber(u16, &writer.interface, @intCast(key.len));
            try writer.interface.writeAll(key);
            try utils.writeNumber(u64, &writer.interface, block_offset);
            index_offsets[i] = index_record_offset;
            index_record_offset += 10 + @as(u32, @intCast(key.len));
        }
        for (index_offsets) |index_offset| {
            try utils.writeNumber(u32, &writer.interface, index_offset);
        }
        self.index_records_num = @intCast(blocks_number);
        try utils.writeNumber(u32, &writer.interface, self.index_records_num);
    }

    inline fn writeBloom(self: *SSTable, writer: *FileWriter) !void {
        self.bloom_start_offset = @intCast(writer.pos);
        try writer.interface.writeAll(self.bloom_filter.?.filter);
    }

    inline fn writeMinMaxKeys(self: *SSTable, writer: *FileWriter) !void {
        const block_buffer = getWorkerContext().?.block_buffer;
        var reader = self.file.reader(getWorkerContext().?.reader_buffer[0..2]);

        try reader.seekTo(0);
        _ = try reader.interface.readSliceAll(block_buffer);
        const min_key = readKeyFromBuffer(block_buffer, 0);

        self.min_key_start_offset = @intCast(writer.pos);
        try utils.writeNumber(u16, &writer.interface, @intCast(min_key.len));
        try writer.interface.writeAll(min_key);

        self.max_key_start_offset = @intCast(writer.pos);
        try utils.writeNumber(u16, &writer.interface, @intCast(self.max_key.?.len));
        try writer.interface.writeAll(self.max_key.?);
    }

    inline fn writeFooter(self: *SSTable, writer: *FileWriter) !void {
        try utils.writeNumber(u64, &writer.interface, self.index_start_offset);
        try utils.writeNumber(u64, &writer.interface, self.bloom_start_offset);
        try utils.writeNumber(u64, &writer.interface, self.min_key_start_offset);
        try utils.writeNumber(u64, &writer.interface, self.max_key_start_offset);
        try utils.writeNumber(u32, &writer.interface, self.records_number);
        try utils.writeNumber(u32, &writer.interface, self.block_size);
    }

    pub fn open(allocator: std.mem.Allocator, handle: FileHandle) !SSTable {
        const path = try fileNameFromHandle(allocator, global_context.getRootFolder(), handle);
        defer allocator.free(path);

        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
        const file_max_position = try file.getEndPos();
        var reader = file.reader(getWorkerContext().?.reader_buffer[0..]);

        try reader.seekTo(file_max_position - 4);
        const block_size = try utils.readNumber(u32, &reader.interface);
        try reader.seekTo(file_max_position - 8);
        const records_number = try utils.readNumber(u32, &reader.interface);
        try reader.seekTo(file_max_position - 16);
        const max_key_start_offset = try utils.readNumber(u64, &reader.interface);
        try reader.seekTo(file_max_position - 24);
        const min_key_start_offset = try utils.readNumber(u64, &reader.interface);

        try reader.seekTo(file_max_position - 32);
        const bloom_start_offset = try utils.readNumber(u64, &reader.interface);
        const bloom_size: u64 = min_key_start_offset - bloom_start_offset;
        const filter = try allocator.alloc(u8, bloom_size);

        try reader.seekTo(file_max_position - 40);
        const index_start_offset: u64 = try utils.readNumber(u64, &reader.interface);

        try reader.seekTo(@intCast(bloom_start_offset - 4));
        const index_records_num: u32 = try utils.readNumber(u32, &reader.interface);

        try reader.seekTo(bloom_start_offset);
        _ = try reader.interface.readSliceAll(filter);

        const min_key_buf = try readKeyFromOffset(allocator, &reader, min_key_start_offset);
        const max_key_buf = try readKeyFromOffset(allocator, &reader, max_key_start_offset);
        const index = try allocator.alloc(u8, bloom_start_offset - index_start_offset);
        try reader.seekTo(index_start_offset);
        _ = try reader.interface.readSliceAll(index);

        return SSTable{
            .allocator = allocator,
            .handle = handle,
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

    pub fn find(self: *const SSTable, key: BinaryData) !?BinaryData {
        if (self.bloom_filter.?.mayContain(key) == false) return null;
        if (utils.compareBitwise(key, self.min_key.?) < 0) return null;
        if (utils.compareBitwise(key, self.max_key.?) > 0) return null;

        const search_result = try self.findBlockOffset(key);
        if (search_result) |offset| {
            return try self.findInBlock(key, offset);
        }
        return null;
    }

    pub fn iterator(self: *SSTable) !Iterator {
        return try Iterator.init(self.allocator, self);
    }

    fn findBlockOffset(self: *const SSTable, key: BinaryData) !?u64 {
        var low: u32 = 0;
        var high: u32 = self.index_records_num - 1;
        while (low <= high) {
            const high_index_record_offset: u32 = try self.readIndexRecordOffset(high);
            const high_index_record_min_key = readKeyFromBuffer(self.index_buffer.?, high_index_record_offset);

            if (utils.compareBitwise(key, high_index_record_min_key) >= 0) {
                const data_block_offset: u64 = utils.intFromBytes(
                    u64,
                    self.index_buffer.?,
                    high_index_record_offset + 2 + high_index_record_min_key.len,
                );
                return data_block_offset;
            }

            const mid = low + (high - low) / 2;
            const index_record_offset: u32 = try self.readIndexRecordOffset(mid);
            if (mid == low) {
                const key_size: u16 = utils.intFromBytes(u16, self.index_buffer.?, index_record_offset);
                const data_block_offset: u64 = utils.intFromBytes(
                    u64,
                    self.index_buffer.?,
                    index_record_offset + 2 + key_size,
                );
                return data_block_offset;
            }

            const index_record_min_key = readKeyFromBuffer(self.index_buffer.?, index_record_offset);
            const compare_result = utils.compareBitwise(key, index_record_min_key);
            if (compare_result == 0) {
                const data_block_offset: u64 = utils.intFromBytes(
                    u64,
                    self.index_buffer.?,
                    index_record_offset + 2 + index_record_min_key.len,
                );
                return data_block_offset;
            } else if (compare_result < 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return null;
    }

    fn findInBlock(self: *const SSTable, key: BinaryData, block_offset: u64) !?BinaryData {
        const block_buffer: []u8 = getWorkerContext().?.block_buffer;
        _ = try self.file.pread(block_buffer, block_offset);

        const num_elements: u32 = utils.intFromBytes(u32, block_buffer, block_buffer.len - 4);
        var low: u32 = 0;
        var high: u32 = num_elements - 1;
        while (low <= high) {
            const mid = low + (high - low) / 2;
            const record_offset: u32 = utils.intFromBytes(
                u32,
                block_buffer,
                block_buffer.len - 4 - 4 * (num_elements - mid),
            );
            const record = StorageRecord.fromBytes(block_buffer, record_offset);
            const compare_result = utils.compareBitwise(key, record.key.data);
            if (compare_result == 0) {
                if (record.isExpired()) return null;

                if (record.value.data.len > 0) {
                    const result = try self.allocator.alloc(u8, record.value.data.len);
                    @memcpy(result, record.value.data);
                    return result;
                } else {
                    return data_types.EMPTY_VALUE;
                }
            } else if (compare_result < 0) {
                if (mid == 0) break;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return null;
    }

    inline fn readKeyFromOffset(allocator: std.mem.Allocator, reader: *std.fs.File.Reader, offset: u64) ![]const u8 {
        try reader.seekTo(offset);
        const key_size: u16 = try utils.readNumber(u16, &reader.interface);
        try reader.seekTo(offset + 2);
        const key: []u8 = try allocator.alloc(u8, key_size);
        _ = try reader.interface.readSliceAll(key);
        return key;
    }

    inline fn readKeyFromBuffer(index_buffer: []const u8, offset: u32) []const u8 {
        const key_size: u16 = utils.intFromBytes(u16, index_buffer, offset);
        return index_buffer[offset + 2 .. offset + 2 + key_size];
    }

    inline fn readIndexRecordOffset(self: *const SSTable, record_number: u32) !u32 {
        return utils.intFromBytes(u32, self.index_buffer.?, self.index_buffer.?.len - 4 - 4 * (self.index_records_num - record_number));
    }
};

// Tests
const testing = std.testing;

fn cleanup(file: SSTable, memtable: Memtable) !void {
    const path = try fileNameFromHandle(testing.allocator, global_context.getRootFolder(), file.handle);
    defer testing.allocator.free(path);

    try std.fs.cwd().deleteFile(path);
    try memtable.wal.deleteFile();
}

test "SSTable#create" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 86;

    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value: [1]u8 = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (254..264) |i| {
        slot = test_memtable.reserve(22);
        try test_memtable.add(
            &.{
                .data = &utils.intToBytes(usize, i),
                .timestamp = std.time.milliTimestamp(),
            },
            &.{
                .data = &test_value,
                .flags = 0,
                .ttl = null,
            },
            &slot.?,
        );
    }

    var adapter = @import("./memtable.zig").MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(
        testing.allocator,
        &memtable_iterator,
        test_memtable.size,
        .{ .level = 0, .file_id = 0 },
        block_size,
        20,
    );
    try testing.expect(test_sstable.index_start_offset == 0x158);
    try testing.expect(test_sstable.bloom_start_offset == 0x1b4);
    try testing.expect(test_sstable.min_key_start_offset == 0x1ce);
    try testing.expect(test_sstable.max_key_start_offset == 0x1d8);
    try testing.expect(test_sstable.records_number == 10);
    try testing.expect(test_sstable.block_size == block_size);
    try testing.expect(test_sstable.index_records_num == 4);

    test_sstable.close(false);
    try cleanup(test_sstable, test_memtable);
}

test "SSTable#open" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 86;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (254..264) |i| {
        slot = test_memtable.reserve(22);
        try test_memtable.add(&.{
            .data = &utils.intToBytes(usize, i),
            .timestamp = std.time.milliTimestamp(),
        }, &.{
            .data = &test_value,
            .flags = 0,
            .ttl = null,
        }, &slot.?);
    }

    var adapter = @import("./memtable.zig").MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(
        testing.allocator,
        &memtable_iterator,
        test_memtable.size,
        .{ .level = 0, .file_id = 0 },
        block_size,
        20,
    );
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, .{ .level = 0, .file_id = 0 });

    try testing.expect(std.mem.eql(u8, test_sstable.min_key.?, &utils.intToBytes(usize, 254)));
    try testing.expect(std.mem.eql(u8, test_sstable.max_key.?, &utils.intToBytes(usize, 263)));
    try testing.expect(test_sstable.index_start_offset == 0x158);
    try testing.expect(test_sstable.bloom_start_offset == 0x1b4);
    try testing.expect(test_sstable.min_key_start_offset == 0x1ce);
    try testing.expect(test_sstable.max_key_start_offset == 0x1d8);
    try testing.expect(test_sstable.records_number == 10);
    try testing.expect(test_sstable.block_size == block_size);
    try testing.expect(test_sstable.index_records_num == 4);

    test_sstable.close(true);
    try cleanup(test_sstable, test_memtable);
}

test "SSTable#find" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 80;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (0..10) |i| {
        slot = test_memtable.reserve(22);
        try test_memtable.add(
            &.{
                .data = &utils.intToBytes(usize, i * 3),
                .timestamp = std.time.milliTimestamp(),
            },
            &.{
                .data = &test_value,
                .flags = 0,
                .ttl = null,
            },
            &slot.?,
        );
    }

    var adapter = @import("./memtable.zig").MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(
        testing.allocator,
        &memtable_iterator,
        test_memtable.size,
        .{ .level = 0, .file_id = 0 },
        block_size,
        0,
    );
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, .{ .level = 0, .file_id = 0 });
    defer test_sstable.close(true);

    for (0..30) |i| {
        const result = try test_sstable.find(&utils.intToBytes(usize, i));
        defer {
            if (result) |r| testing.allocator.free(r);
        }

        if (i % 3 != 0) {
            try testing.expect(result == null);
        } else {
            try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u8, 0)));
        }
    }

    const nvs = [2]usize{ 200, 300 };
    for (nvs) |v| {
        try testing.expect(try test_sstable.find(&utils.intToBytes(usize, v)) == null);
    }

    try cleanup(test_sstable, test_memtable);
}

test "SSTable#iterator" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 100;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (0..10) |i| {
        slot = test_memtable.reserve(22);
        try test_memtable.add(
            &.{
                .data = &utils.intToBytes(usize, i),
                .timestamp = std.time.milliTimestamp(),
            },
            &.{
                .data = &test_value,
                .flags = 0,
                .ttl = null,
            },
            &slot.?,
        );
    }

    var adapter = @import("./memtable.zig").MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(
        testing.allocator,
        &memtable_iterator,
        test_memtable.size,
        .{ .level = 0, .file_id = 0 },
        block_size,
        20,
    );
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, .{ .level = 0, .file_id = 0 });
    defer test_sstable.close(true);

    var iterator = try test_sstable.iterator();
    defer iterator.deinit();

    var expected_key: usize = 0;
    while (try iterator.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key.data, &utils.intToBytes(usize, expected_key)));
        try testing.expect(std.mem.eql(u8, record.value.data, &utils.intToBytes(u8, 0)));
        expected_key += 1;
    }

    try cleanup(test_sstable, test_memtable);
}

test "SSTable#iterator with range" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    const block_size: u32 = 134;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 69632, 8, "./");
    defer test_memtable.destroy();

    const test_value = utils.intToBytes(u8, 0);
    var slot: ?Memtable.ReservedDataSlot = null;
    for (0..18) |i| {
        slot = test_memtable.reserve(22);
        try test_memtable.add(
            &.{
                .data = &utils.intToBytes(usize, i),
                .timestamp = std.time.milliTimestamp(),
            },
            &.{
                .data = &test_value,
                .flags = 0,
                .ttl = null,
            },
            &slot.?,
        );
    }

    var adapter = @import("./memtable.zig").MemtableIteratorAdapter.init(&test_memtable);
    var memtable_iterator = adapter.iterator();

    var test_sstable = try SSTable.create(
        testing.allocator,
        &memtable_iterator,
        test_memtable.size,
        .{ .level = 0, .file_id = 0 },
        block_size,
        20,
    );
    test_sstable.close(false);

    test_sstable = try SSTable.open(testing.allocator, .{ .level = 0, .file_id = 0 });
    defer test_sstable.close(true);

    var iterator = try test_sstable.iterator();
    defer iterator.deinit();

    var range = BinaryDataRange{
        .start = &utils.intToBytes(usize, 5),
        .end = &utils.intToBytes(usize, 12),
    };

    try iterator.setRange(range);
    var expected_key: usize = 5;
    while (try iterator.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key.data, &utils.intToBytes(usize, expected_key)));
        try testing.expect(std.mem.eql(u8, record.value.data, &utils.intToBytes(u8, 0)));
        try testing.expect(utils.intFromBytes(usize, record.key.data, 0) >= 5);
        try testing.expect(utils.intFromBytes(usize, record.key.data, 0) <= 12);
        expected_key += 1;
    }

    range = BinaryDataRange{
        .start = &utils.intToBytes(usize, 10),
        .end = &utils.intToBytes(usize, 18),
    };

    try iterator.setRange(range);
    expected_key = 10;
    while (try iterator.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key.data, &utils.intToBytes(usize, expected_key)));
        try testing.expect(std.mem.eql(u8, record.value.data, &utils.intToBytes(u8, 0)));
        try testing.expect(utils.intFromBytes(usize, record.key.data, 0) >= 10);
        try testing.expect(utils.intFromBytes(usize, record.key.data, 0) <= 18);
        expected_key += 1;
    }

    range = BinaryDataRange{
        .start = &utils.intToBytes(usize, 7),
        .end = &utils.intToBytes(usize, 16),
    };

    try iterator.setRange(range);
    expected_key = 7;
    while (try iterator.next()) |record| {
        try testing.expect(std.mem.eql(u8, record.key.data, &utils.intToBytes(usize, expected_key)));
        try testing.expect(std.mem.eql(u8, record.value.data, &utils.intToBytes(u8, 0)));
        try testing.expect(utils.intFromBytes(usize, record.key.data, 0) >= 7);
        try testing.expect(utils.intFromBytes(usize, record.key.data, 0) <= 16);
        expected_key += 1;
    }

    try cleanup(test_sstable, test_memtable);
}
