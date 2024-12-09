const std = @import("std");
const data_types = @import("./data_types.zig");
const utils = @import("./utils.zig");

const Storage = @import("./storage.zig").Storage;
const NodeIndexStorage = @import("./node_index_storage.zig").NodeIndexStorage;

const ValueType = data_types.ValueType;

pub const NodeRecord = struct {
    value_size: u16,
    value_type: ValueType,
    value: []const u8,
    node_id: u64,
};

pub const NodeStorage = struct {
    allocator: std.mem.Allocator,
    storage: Storage(u64),
    node_index_storage: NodeIndexStorage,

    pub fn init(path: []const u8, max_memtable_size: u16, allocator: std.mem.Allocator) !NodeStorage {
        var node_index_storage = NodeIndexStorage{};
        try node_index_storage.open();

        return NodeStorage{
            .allocator = allocator,
            .storage = try Storage(u64).start(path, max_memtable_size, allocator),
            .node_index_storage = node_index_storage,
        };
    }

    pub inline fn stop(self: *NodeStorage) void {
        self.node_index_storage.close();
        self.storage.stop();
    }

    pub fn put(self: *NodeStorage, node_value: []const u8, node_type: ValueType) !void {
        const node_key = try self.build_key(node_value, node_type);
        defer self.allocator.free(node_key);

        const found_value = try self.storage.find(node_key);
        if (found_value != null) return;

        const node_id = try self.node_index_storage.allocate_next_id();
        try self.storage.put(node_key, node_id);
    }

    pub fn find(self: *NodeStorage, node_value: []const u8, node_type: ValueType) !?u64 {
        const node_key = try self.build_key(node_value, node_type);
        defer self.allocator.free(node_key);

        return try self.storage.find(node_key);
    }

    inline fn build_key(self: *const NodeStorage, node_bytes: []const u8, node_type: ValueType) ![]u8 {
        const key_buffer = try self.allocator.alloc(u8, 1 + node_bytes.len);
        key_buffer[0] = @intFromEnum(node_type);
        @memcpy(key_buffer[1..], node_bytes);
        return key_buffer;
    }
};

// Tests
const testing = std.testing;

inline fn to_byte_array(comptime T: type, node: T) ![@sizeOf(@TypeOf(node))]u8 {
    return switch (T) {
        bool => utils.key_from_int_data(@as(u8, if (node == true) 1 else 0)),
        i8, i16, i32, i64, u8, u16, u32, u64 => utils.key_from_int_data(T, node),
        f32 => utils.key_from_int_data(u32, @as(u32, @bitCast(node))),
        f64 => utils.key_from_int_data(u64, @as(u64, @bitCast(node))),
        else => error.DataError,
    };
}

fn clean_up(node_storage: *NodeStorage) void {
    for (node_storage.storage.memtables.items) |t| {
        t.wal.delete_file() catch {
            const out = std.io.getStdOut().writer();
            std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
        };
    }
    var it = node_storage.storage.table_file_manager.files.iterator();
    while (it.next()) |entry| {
        for (entry.value_ptr.*.items) |file_name| {
            std.fs.cwd().deleteFile(file_name) catch {
                const out = std.io.getStdOut().writer();
                std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
            };
        }
    }
    std.fs.cwd().deleteFile("./node_index.store") catch {
        const out = std.io.getStdOut().writer();
        std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
    };
}

test "NodeStorage#put" {
    var node_storage = try NodeStorage.init("./", 4, testing.allocator);
    defer node_storage.stop();
    defer clean_up(&node_storage);

    const v1 = try to_byte_array(u8, 2);
    try node_storage.put(&v1, ValueType.smallserial);

    const v2 = try to_byte_array(u64, 2);
    try node_storage.put(&v2, ValueType.bigserial);

    const v3 = try to_byte_array(i32, -5);
    try node_storage.put(&v3, ValueType.int);

    const v4 = try to_byte_array(f32, 12.34);
    try node_storage.put(&v4, ValueType.float);

    try node_storage.put("Hello, world!", ValueType.string);
}

test "Add value twice" {
    var node_storage = try NodeStorage.init("./", 4, testing.allocator);
    defer node_storage.stop();
    defer clean_up(&node_storage);

    for (1..10) |i| {
        const v: u8 = @as(u8, @intCast(i));
        try node_storage.put(&utils.key_from_int_data(u8, v), ValueType.smallserial);
    }
    const initial_memtable_size = node_storage.storage.memtables.getLast().size;

    const v: u8 = @as(u8, @intCast(5));
    try node_storage.put(&utils.key_from_int_data(u8, v), ValueType.smallserial);
    try testing.expect(node_storage.storage.memtables.getLast().size == initial_memtable_size);
}
