const std = @import("std");
const utils = @import("./utils.zig");
const BinaryStorage = @import("./binary_storage.zig").BinaryStorage;
const ValueType = @import("./data_types.zig").ValueType;
const BinaryData = @import("./data_types.zig").BinaryData;
const TypedBinaryData = @import("./data_types.zig").TypedBinaryData;

const DATABASE_STORAGE = ".hodag";

pub const TypedStorage = struct {
    allocator: std.mem.Allocator,
    storage: BinaryStorage,

    pub fn init(allocator: std.mem.Allocator, max_memtable_size: u16) !TypedStorage {
        try utils.make_dir_if_not_exists(DATABASE_STORAGE);

        return .{
            .allocator = allocator,
            .storage = try BinaryStorage.start(allocator, DATABASE_STORAGE, max_memtable_size),
        };
    }

    pub inline fn stop(self: *TypedStorage) void {
        self.storage.stop();
    }

    pub fn set(self: *TypedStorage, key: TypedBinaryData, value: TypedBinaryData) !void {
        const key_bytes = try self.build_key(key.data_type, key.data);
        defer self.allocator.free(key_bytes);
        const value_bytes = try self.build_key(value.data_type, value.data);
        defer self.allocator.free(value_bytes);
        try self.storage.put(key_bytes, value_bytes);
    }

    pub fn get(self: *TypedStorage, key: TypedBinaryData) !?TypedBinaryData {
        const binary_key = try self.build_key(key.data_type, key.data);
        defer self.allocator.free(binary_key);

        const result = try self.storage.find(binary_key);
        if (result) |r| {
            defer self.storage.allocator.free(r);
            return try TypedBinaryData.from_bytes(self.allocator, r);
        }

        return null;
    }

    inline fn build_key(self: *const TypedStorage, node_type: ValueType, node_bytes: []const u8) ![]u8 {
        const key_buffer = try self.allocator.alloc(u8, 1 + node_bytes.len);
        key_buffer[0] = @intFromEnum(node_type);
        @memcpy(key_buffer[1..], node_bytes);
        return key_buffer;
    }
};

// Tests
const testing = std.testing;
const global_context = @import("./global_context.zig");
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;

fn clean_up(typed_storage: *TypedStorage) void {
    for (typed_storage.storage.memtables.items) |t| {
        t.wal.delete_file() catch {
            const out = std.io.getStdOut().writer();
            std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
        };
    }
    var it = typed_storage.storage.table_file_manager.files.iterator();
    while (it.next()) |entry| {
        for (entry.value_ptr.*.items) |file_name| {
            std.fs.cwd().deleteFile(file_name) catch {
                const out = std.io.getStdOut().writer();
                std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
            };
        }
    }
}

inline fn build_typed_data(comptime T: type, value_type: ValueType, value: T) !TypedBinaryData {
    return .{
        .allocator = testing.allocator,
        .data_type = value_type,
        .data = &(try utils.to_bytes(T, value)),
    };
}

test "NodeStorage#set" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var test_storage = try TypedStorage.init(testing.allocator, 4);
    defer test_storage.stop();
    defer clean_up(&test_storage);

    try test_storage.set(
        try build_typed_data(u8, .smallint, 2),
        try build_typed_data(u8, .smallint, 2),
    );
    try testing.expect(test_storage.storage.memtables.items[0].size == 1);

    try test_storage.set(
        try build_typed_data(u64, .bigserial, 2),
        try build_typed_data(u64, .bigserial, 2),
    );
    try testing.expect(test_storage.storage.memtables.items[0].size == 2);

    try test_storage.set(
        try build_typed_data(i32, .int, -5),
        try build_typed_data(i32, .int, 5),
    );
    try testing.expect(test_storage.storage.memtables.items[0].size == 3);

    try test_storage.set(
        try build_typed_data(f32, .float, -5.5),
        try build_typed_data(f32, .float, 5.5),
    );
    try testing.expect(test_storage.storage.memtables.items[0].size == 4);

    const data = TypedBinaryData{
        .allocator = testing.allocator,
        .data_type = .string,
        .data = "Hello, world!",
    };
    try test_storage.set(data, data);
    try testing.expect(test_storage.storage.memtables.items[0].size == 1);
}

test "NodeStorage#get" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var test_storage = try TypedStorage.init(testing.allocator, 4);
    defer test_storage.stop();
    defer clean_up(&test_storage);

    const key = try build_typed_data(u8, .smallint, 2);
    const value = try build_typed_data(f32, .float, 1.23);
    try test_storage.set(key, value);
    const result = try test_storage.get(try build_typed_data(u8, .smallint, 2));
    defer testing.allocator.free(result.?.data);

    try testing.expect(std.mem.eql(u8, result.?.data, value.data));
}
