const std = @import("std");
const data_types = @import("./data_types.zig");
const m = @import("./memtable.zig");
const ValueType = data_types.ValueType;

pub const SSTable = struct {
    path: []const u8,
    file: std.fs.File,

    pub fn create(comptime N: u8, memtable: *m.Memtable(N), path: []const u8) !SSTable {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        const sstable = SSTable{
            .path = path,
            .file = file,
        };

        var it = memtable.*.interator();
        while (it.next()) |node| {
            try sstable.write(@TypeOf(node.value.?.node_id), node.value.?.node_id);
            const value_type = @intFromEnum(node.value.?.value_type);
            try sstable.write(@TypeOf(value_type), value_type);
            try sstable.write(@TypeOf(node.value.?.value_size), node.value.?.value_size);
            try file.writeAll(node.key.?.items);
        }

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

const TEST_SSTABLE_PATH = "./node_store.store";

inline fn test_value() m.MemtableValue {
    return m.MemtableValue{
        .node_id = 0,
        .value_size = 8,
        .value_type = ValueType.bigserial,
    };
}

fn clean_up(storage: SSTable) !void {
    std.fs.cwd().deleteFile(storage.path) catch {
        const out = std.io.getStdOut().writer();
        try std.fmt.format(out, "{s}", .{"failed to clean up after the test\n"});
    };
}

test "SSTable#create" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = m.Memtable(8).init(testing.allocator, rng.random(), 0.125);
    defer test_memtable.destroy();

    const test_vertex_data = test_value();
    for (254..264) |i| {
        try test_memtable.add(&m.key_from_int_data(usize, i), test_vertex_data);
    }

    const test_sstable = try SSTable.create(8, &test_memtable, TEST_SSTABLE_PATH);
    test_sstable.close();
    try clean_up(test_sstable);
}
