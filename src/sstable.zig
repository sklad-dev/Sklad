const std = @import("std");
const data_types = @import("./data_types.zig");
const m = @import("./memtable.zig");
const b = @import("./bloom.zig");
const ValueType = data_types.ValueType;
const NodeRecord = data_types.NodeRecord;

pub const SSTable = struct {
    path: []const u8,
    file: std.fs.File,

    pub fn create(comptime N: u8, memtable: *m.Memtable(N), path: []const u8, sparse_index_step: u32, allocator: std.mem.Allocator) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        const sstable = SSTable{
            .path = path,
            .file = file,
        };

        const fixed_record_length: u32 = 64 + 8 + 16;

        var offsets = std.ArrayList(u32).init(allocator);
        defer offsets.deinit();

        var bloom_filter = try b.BloomFilter.init(@intCast(memtable.size), 20, allocator);
        defer bloom_filter.deinit();

        var i: u16 = 0;
        var it = memtable.*.iterator();
        var min_key: []u8 = undefined;
        var max_key: []u8 = undefined;
        while (it.next()) |node| {
            if (i == 0) min_key = node.key.?;
            if (i == memtable.size - 1) max_key = node.key.?;

            const prev_offset: u32 = if (offsets.getLastOrNull() != null) offsets.getLast() else 0;
            const current_offset: u32 = @intCast(try file.getPos());
            const record_size: u32 = (fixed_record_length + node.value.?.value_size) / 8;
            if ((current_offset - prev_offset) + record_size > sparse_index_step) {
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

        try sstable.write(u16, @as(u16, @intCast(min_key.len)));
        try file.writeAll(min_key);
        try sstable.write(u16, @as(u16, @intCast(max_key.len)));
        try file.writeAll(max_key);

        const bloom_start_offset: u32 = @intCast(try file.getPos());
        try file.writeAll(bloom_filter.filter);

        try sstable.write(u32, bloom_start_offset);
        try sstable.write(u32, index_start_offset);
        try sstable.write(u32, index_end_offset);

        return sstable;
    }

    pub inline fn close(self: SSTable) void {
        self.file.close();
    }

    inline fn write(self: SSTable, comptime T: type, data: T) !void {
        var buffer: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buffer, data, std.builtin.Endian.big);
        try self.file.writeAll(&buffer);
    }
};

// Tests
const testing = std.testing;
const utils = @import("./utils.zig");

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

    const test_sstable = try SSTable.create(8, &test_memtable, TEST_SSTABLE_PATH, 76, testing.allocator);
    test_sstable.close();
    try clean_up(8, test_sstable, test_memtable);
}
