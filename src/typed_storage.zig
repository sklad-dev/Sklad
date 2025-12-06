const std = @import("std");

const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");
const BinaryStorage = @import("./binary_storage.zig").BinaryStorage;
const ValueType = @import("./data_types.zig").ValueType;
const BinaryData = @import("./data_types.zig").BinaryData;
const TypedBinaryData = @import("./data_types.zig").TypedBinaryData;

pub const TypedStorage = struct {
    allocator: std.mem.Allocator,
    storage: BinaryStorage,

    pub fn init(allocator: std.mem.Allocator) !TypedStorage {
        try utils.makeDirIfNotExists(global_context.getRootFolder());

        return .{
            .allocator = allocator,
            .storage = try BinaryStorage.start(allocator, global_context.getRootFolder()),
        };
    }

    pub inline fn stop(self: *TypedStorage) void {
        self.storage.stop();
    }

    pub fn set(self: *TypedStorage, key: TypedBinaryData, value: TypedBinaryData) !void {
        const key_bytes = try key.toBytes();
        defer key.allocator.free(key_bytes);
        const value_bytes = try value.toBytes();
        defer value.allocator.free(value_bytes);
        try self.storage.put(key_bytes, value_bytes);
    }

    pub fn get(self: *TypedStorage, key: TypedBinaryData) !?TypedBinaryData {
        const key_bytes = try key.toBytes();
        defer key.allocator.free(key_bytes);

        const result = try self.storage.find(key_bytes);
        if (result) |r| {
            defer self.storage.allocator.free(r);
            return try TypedBinaryData.fromBytes(self.allocator, r);
        }

        return null;
    }
};

// Tests
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

fn cleanup(typed_storage: *TypedStorage) !void {
    if (typed_storage.storage.pending_memtables.load(.acquire)) |list| {
        _ = list.acquire();
        defer _ = list.release();

        var curr = list.get().head.next;
        while (curr) |node| : (curr = node.next) {
            if (node.entry) |entry| {
                try entry.memtable.wal.deleteFile();
            }
        }
    }

    for (0..typed_storage.storage.table_file_manager.files.len) |level| {
        if (typed_storage.storage.table_file_manager.acquireFilesAtLevel(@intCast(level))) |files| {
            defer _ = files.release();

            var current = files.get().head.next;
            while (current) |node| : (current = node.next) {
                if (node.entry) |file_id| {
                    var cached_record = try typed_storage.storage.sstable_cache.get(.{
                        .level = @intCast(level),
                        .id = file_id,
                    }, &typed_storage.storage.table_file_manager);
                    defer _ = cached_record.?.release();
                }
            }
        }
    }

    var dir = try std.fs.cwd().openDir("./", .{
        .access_sub_paths = false,
        .iterate = true,
        .no_follow = true,
    });
    defer dir.close();

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind == .file and (std.mem.endsWith(
            u8,
            entry.name,
            ".sstable",
        ) or std.mem.endsWith(
            u8,
            entry.name,
            ".wal",
        ))) {
            try std.fs.cwd().deleteFile(entry.name);
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
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    defer global_context.deinitConfigurationForTests();

    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var test_storage = try TypedStorage.init(testing.allocator);
    defer test_storage.stop();

    try test_storage.set(
        try buildTypedData(u8, .smallint, 2),
        try buildTypedData(u8, .smallint, 2),
    );
    try testing.expect(test_storage.storage.active_memtable.load(.unordered).size == 1);

    try test_storage.set(
        try buildTypedData(u64, .bigserial, 2),
        try buildTypedData(u64, .bigserial, 2),
    );
    try testing.expect(test_storage.storage.active_memtable.load(.unordered).size == 2);

    try test_storage.set(
        try buildTypedData(i32, .int, -5),
        try buildTypedData(i32, .int, 5),
    );
    try testing.expect(test_storage.storage.active_memtable.load(.unordered).size == 3);

    try test_storage.set(
        try buildTypedData(f32, .float, -5.5),
        try buildTypedData(f32, .float, 5.5),
    );
    try testing.expect(test_storage.storage.active_memtable.load(.unordered).size == 4);

    const data = TypedBinaryData{
        .allocator = testing.allocator,
        .data_type = .string,
        .data = "Hello, world!",
    };
    try test_storage.set(data, data);
    try testing.expect(test_storage.storage.active_memtable.load(.unordered).size == 5);

    try cleanup(&test_storage);
}

test "NodeStorage#get" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    defer global_context.deinitConfigurationForTests();

    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

    var test_storage = try TypedStorage.init(testing.allocator);
    defer test_storage.stop();

    const key = try buildTypedData(u8, .smallint, 2);
    const value = try buildTypedData(f32, .float, 1.23);
    try test_storage.set(key, value);
    const result = try test_storage.get(try buildTypedData(u8, .smallint, 2));
    defer testing.allocator.free(result.?.data);

    try testing.expect(std.mem.eql(u8, result.?.data, value.data));

    try cleanup(&test_storage);
}
