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

    pub fn init(allocator: std.mem.Allocator) !TypedStorage {
        try utils.makeDirIfNotExists(DATABASE_STORAGE);

        return .{
            .allocator = allocator,
            .storage = try BinaryStorage.start(allocator, DATABASE_STORAGE),
        };
    }

    pub inline fn stop(self: *TypedStorage) void {
        self.storage.stop();
    }

    pub fn set(self: *TypedStorage, key: TypedBinaryData, value: TypedBinaryData) !void {
        const key_bytes = try self.build_binary_data(key.data_type, key.data);
        defer self.allocator.free(key_bytes);
        const value_bytes = try self.build_binary_data(value.data_type, value.data);
        defer self.allocator.free(value_bytes);
        try self.storage.put(key_bytes, value_bytes);
    }

    pub fn get(self: *TypedStorage, key: TypedBinaryData) !?TypedBinaryData {
        const binary_key = try self.build_binary_data(key.data_type, key.data);
        defer self.allocator.free(binary_key);

        const result = try self.storage.find(binary_key);
        if (result) |r| {
            defer self.storage.allocator.free(r);
            return try TypedBinaryData.fromBytes(self.allocator, r);
        }

        return null;
    }

    inline fn build_binary_data(self: *const TypedStorage, node_type: ValueType, node_bytes: []const u8) ![]u8 {
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
const TaskQueue = @import("./task_queue.zig").TaskQueue;

fn cleanup(typed_storage: *TypedStorage) void {
    var iter = typed_storage.storage.memtables.iterator();
    defer iter.deinit();

    typed_storage.storage.active_memtable.wal.deleteFile() catch {
        const out = std.io.getStdOut().writer();
        std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
        return;
    };

    while (iter.next()) |node| {
        node.entry.?.memtable.wal.deleteFile() catch {
            const out = std.io.getStdOut().writer();
            std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
        };
    }

    for (0..typed_storage.storage.table_file_manager.files.len) |level| {
        if (typed_storage.storage.table_file_manager.files[level]) |files| {
            var it = files.iterator();
            defer it.deinit();
            while (it.next()) |node| {
                const file_name = node.entry.?.*;
                std.fs.cwd().deleteFile(file_name) catch {
                    const out = std.io.getStdOut().writer();
                    std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
                };
            }
        }
    }
}

inline fn buildTypedData(comptime T: type, value_type: ValueType, value: T) !TypedBinaryData {
    return .{
        .allocator = testing.allocator,
        .data_type = value_type,
        .data = &(try utils.toBytes(T, value)),
    };
}

test "NodeStorage#set" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinitConfigurationForTests();

    configurator.* = TestingConfigurator.init();
    configurator.max_size = 4;
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var test_storage = try TypedStorage.init(testing.allocator);
    defer test_storage.stop();
    defer cleanup(&test_storage);

    try test_storage.set(
        try buildTypedData(u8, .smallint, 2),
        try buildTypedData(u8, .smallint, 2),
    );
    try testing.expect(test_storage.storage.active_memtable.size == 1);

    try test_storage.set(
        try buildTypedData(u64, .bigserial, 2),
        try buildTypedData(u64, .bigserial, 2),
    );
    try testing.expect(test_storage.storage.active_memtable.size == 2);

    try test_storage.set(
        try buildTypedData(i32, .int, -5),
        try buildTypedData(i32, .int, 5),
    );
    try testing.expect(test_storage.storage.active_memtable.size == 3);

    try test_storage.set(
        try buildTypedData(f32, .float, -5.5),
        try buildTypedData(f32, .float, 5.5),
    );
    try testing.expect(test_storage.storage.active_memtable.size == 4);

    const data = TypedBinaryData{
        .allocator = testing.allocator,
        .data_type = .string,
        .data = "Hello, world!",
    };
    try test_storage.set(data, data);
    try testing.expect(test_storage.storage.active_memtable.size == 1);
}

test "NodeStorage#get" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinitConfigurationForTests();

    configurator.* = TestingConfigurator.init();
    configurator.max_size = 4;
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var test_storage = try TypedStorage.init(testing.allocator);
    defer test_storage.stop();
    defer cleanup(&test_storage);

    const key = try buildTypedData(u8, .smallint, 2);
    const value = try buildTypedData(f32, .float, 1.23);
    try test_storage.set(key, value);
    const result = try test_storage.get(try buildTypedData(u8, .smallint, 2));
    defer testing.allocator.free(result.?.data);

    try testing.expect(std.mem.eql(u8, result.?.data, value.data));
}
