const std = @import("std");
const builtin = @import("builtin");

const ArrayList = std.ArrayList;
const AutoHashMap = std.AutoHashMap;
const StringHashMap = std.StringHashMap;

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const DoubleLinkedPairList = @import("./double_linked_pair_list.zig").DoubleLinkedPairList;
const Memtable = @import("./memtable.zig").Memtable;
const SSTable = @import("./sstable.zig").SSTable;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Task = @import("./task_queue.zig").Task;
const Wal = @import("./wal.zig").Wal;

const StorageRecord = data_types.StorageRecord;

pub const BinaryStorage = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    active_memtable: *Memtable,
    memtables: DoubleLinkedPairList(u8, *Memtable),
    memtables_lock: std.Thread.Mutex = .{},
    table_file_manager: TableFileManager,
    tables: StringHashMap(*SSTable),

    const Self = @This();

    pub const FlushTask = struct {
        allocator: std.mem.Allocator,
        memtable_key: u8,
        storage: *BinaryStorage,

        pub fn init(allocator: std.mem.Allocator, memtable_key: u8, storage: *BinaryStorage) !FlushTask {
            return .{
                .allocator = allocator,
                .memtable_key = memtable_key,
                .storage = storage,
            };
        }

        pub fn task(self: *FlushTask) Task {
            return .{
                .context = self,
                .run_fn = FlushTask.run,
                .destroy_fn = FlushTask.destroy,
            };
        }

        fn run(ptr: *anyopaque) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));

            var memtable: ?*Memtable = null;
            if (!utils.try_lock_for(&self.storage.memtables_lock, 200)) {
                std.log.err("Lock timeout: failed to update the memtables", .{});
                return;
            }
            memtable = self.storage.memtables.peek(self.memtable_key);
            self.storage.memtables_lock.unlock();

            self.storage.table_file_manager.flush_memtable(memtable.?) catch |e| {
                std.log.err("Error! Failed to falush a memtable {s}: {any}", .{ memtable.?.wal.path, e });
                return;
            };

            var flushed_memtable: ?*Memtable = null;
            if (!utils.try_lock_for(&self.storage.memtables_lock, 200)) {
                std.log.err("Lock timeout: failed to update the memtables", .{});
                return;
            }
            flushed_memtable = self.storage.memtables.take(self.memtable_key);
            self.storage.memtables_lock.unlock();

            if (flushed_memtable) |mt| {
                mt.destroy();
                self.allocator.destroy(mt);
            }
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }
    };

    pub fn start(allocator: std.mem.Allocator, path: []const u8) !Self {
        var table_file_manager = try TableFileManager.init(allocator, path);
        const storage = Self{
            .allocator = allocator,
            .path = path,
            .active_memtable = try restore_memtables(&table_file_manager),
            .memtables = DoubleLinkedPairList(u8, *Memtable).init(allocator),
            .table_file_manager = table_file_manager,
            .tables = StringHashMap(*SSTable).init(allocator),
        };
        return storage;
    }

    pub inline fn stop(self: *Self) void {
        self.deinit_memtables();
        self.deinit_tables();
        self.table_file_manager.deinit();
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        const record = StorageRecord{
            .allocator = self.allocator,
            .key_size = @as(u16, @intCast(key.len)),
            .key = key,
            .value_size = @as(u16, @intCast(value.len)),
            .value = value,
        };

        var filled_memtable: ?*Memtable = null;
        var filled_memtable_key: u8 = 0;
        if (!utils.try_lock_for(&self.memtables_lock, 200)) return ApplicationError.ExecutionTimeout;
        if (self.active_memtable.is_full()) {
            filled_memtable = self.active_memtable;
            filled_memtable_key = try self.switch_active_memtable();
        }
        self.memtables_lock.unlock();

        try self.active_memtable.wal.write(&record);
        try self.active_memtable.add(key, value);

        if (filled_memtable) |_| {
            const task_queue = global_context.get_task_queue();
            var flush_task = try task_queue.?.allocator.create(FlushTask);

            flush_task.* = FlushTask{
                .allocator = self.allocator,
                .memtable_key = filled_memtable_key,
                .storage = self,
            };

            global_context.get_task_queue().?.enqueue(flush_task.task());
        }
    }

    pub fn find(self: *Self, key: []const u8) !?[]const u8 {
        var value = try self.active_memtable.find(key);
        if (value) |v| {
            const result = try self.allocator.alloc(u8, v.len);
            @memcpy(result, v);
            return result;
        }

        var iter = self.memtables.reverse_iterator();
        while (iter.next()) |m| {
            value = try m.find(key);
            if (value) |v| {
                const result = try self.allocator.alloc(u8, v.len);
                @memcpy(result, v);
                return result;
            }
        }
        return try self.find_in_tables(key);
    }

    fn find_in_tables(self: *Self, key: []const u8) !?[]const u8 {
        var result: ?[]const u8 = null;
        var result_id: i16 = -1;
        var it = self.table_file_manager.files.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.*.items) |file_name| {
                if (self.tables.contains(file_name) == false) {
                    const table = try self.allocator.create(SSTable);
                    table.* = try SSTable.open(file_name, self.allocator);
                    try self.tables.put(
                        file_name,
                        table,
                    );
                }

                if (try self.tables.get(file_name).?.find(key)) |value| {
                    const file_id = try self.table_file_manager.parse_file_id(file_name);
                    if (file_id > result_id) {
                        result_id = file_id;
                        if (result) |r| self.allocator.free(r);
                        result = value;
                    } else {
                        self.allocator.free(value);
                    }
                }
            }
            if (result) |r| {
                return r;
            }
        }
        return null;
    }

    fn switch_active_memtable(self: *Self) !u8 {
        errdefer self.memtables_lock.unlock();

        const memtable = try Memtable.create(self.allocator, self.path);
        const memtable_key: u8 = @as(u8, @intCast(self.memtables.size()));
        try self.memtables.append(memtable_key, self.active_memtable);
        self.active_memtable = memtable;
        return memtable_key;
    }

    fn restore_memtables(table_file_manager: *TableFileManager) !*Memtable {
        var dir = try std.fs.cwd().openDir(table_file_manager.path, .{
            .access_sub_paths = false,
            .iterate = true,
            .no_follow = true,
        });
        defer dir.close();

        var memtable = try Memtable.create(table_file_manager.allocator, table_file_manager.path);

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".wal") and !std.mem.eql(u8, memtable.wal.name(), entry.name)) {
                const wal_name = try table_file_manager.allocator.alloc(u8, table_file_manager.path.len + 9);
                const wal = try Wal.open(
                    table_file_manager.allocator,
                    try std.fmt.bufPrint(
                        wal_name,
                        "{s}/{s}",
                        .{ table_file_manager.path, entry.name },
                    ),
                );
                while (try Memtable.from_wal(wal, memtable) == true) {
                    try table_file_manager.flush_memtable(memtable);
                    memtable.destroy();
                    table_file_manager.allocator.destroy(memtable);
                    memtable = try Memtable.create(table_file_manager.allocator, table_file_manager.path);
                }
                wal.file.close();
                try wal.delete_file();
                wal.allocator.free(wal.path);
            }
        }

        return memtable;
    }

    fn deinit_memtables(self: *Self) void {
        self.active_memtable.destroy();
        self.allocator.destroy(self.active_memtable);

        var iter = self.memtables.iterator();
        while (iter.next()) |t| {
            t.destroy();
            self.allocator.destroy(t);
        }
        self.memtables.deinit();
    }

    fn deinit_tables(self: *Self) void {
        var it = self.tables.valueIterator();
        while (it.next()) |table_ptr| {
            table_ptr.*.*.close();
            self.allocator.destroy(table_ptr.*);
        }
        self.tables.deinit();
    }
};

// Tests
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

fn clean_up(storage: *BinaryStorage) !void {
    try storage.active_memtable.wal.delete_file();

    var iter = storage.memtables.iterator();
    while (iter.next()) |t| {
        try t.wal.delete_file();
    }
    var it = storage.tables.valueIterator();
    while (it.next()) |table_ptr| {
        std.fs.cwd().deleteFile(table_ptr.*.path) catch {
            const out = std.io.getStdOut().writer();
            std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
        };
    }
}

test "BinaryStorage#put" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    configurator.max_size = 4;
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var test_storage = try BinaryStorage.start(testing.allocator, ".");
    defer test_storage.stop();

    try test_storage.put(&utils.int_to_bytes(u8, 1), &utils.int_to_bytes(u8, 42));
    try testing.expect(test_storage.active_memtable.size == 1);
    const result = try test_storage.active_memtable.find(&utils.int_to_bytes(u8, 1));
    try testing.expect(std.mem.eql(u8, result.?, &utils.int_to_bytes(u8, 42)));

    try test_storage.active_memtable.wal.delete_file();
}

test "Restore memtable from wal" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    configurator.max_size = 4;
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var storage1 = try BinaryStorage.start(testing.allocator, ".");
    try storage1.put(&utils.int_to_bytes(u8, 1), &utils.int_to_bytes(u8, 42));
    storage1.stop();

    var storage2 = try BinaryStorage.start(testing.allocator, ".");
    defer storage2.stop();

    try testing.expect(storage2.active_memtable.size == 1);
    const result = try storage2.active_memtable.find(&utils.int_to_bytes(u8, 1));
    try testing.expect(std.mem.eql(u8, result.?, &utils.int_to_bytes(u8, 42)));

    try storage2.active_memtable.wal.delete_file();
}

test "BinaryStorage#find" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    configurator.max_size = 4;
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.init_task_queue_for_tests(&task_queue);
    defer global_context.clean_and_deinit_task_queue_for_tests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    const value = utils.int_to_bytes(u8, 42);
    try storage.put(&utils.int_to_bytes(u8, 1), &value);

    var search_result = try storage.find(&utils.int_to_bytes(u8, 1));
    try testing.expect(std.mem.eql(u8, search_result.?, &value));
    testing.allocator.free(search_result.?);

    search_result = try storage.find(&utils.int_to_bytes(u8, 2));
    try testing.expect(search_result == null);

    for (2..10) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.int_to_bytes(u8, v), &utils.int_to_bytes(u8, v));
    }

    for (1..10) |i| {
        search_result = try storage.find(&utils.int_to_bytes(u8, @as(u8, @intCast(i))));
        defer testing.allocator.free(search_result.?);
        try testing.expect(search_result != null);
    }

    try clean_up(&storage);
}

test "BinaryStorage#find returns the newest value" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    configurator.max_size = 4;
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.init_task_queue_for_tests(&task_queue);
    defer global_context.clean_and_deinit_task_queue_for_tests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.int_to_bytes(u8, v), &utils.int_to_bytes(u8, v));
    }

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.int_to_bytes(u8, v), &utils.int_to_bytes(u8, v * 2));
    }

    const r1 = try storage.find(&utils.int_to_bytes(u8, @as(u8, @intCast(1))));
    defer testing.allocator.free(r1.?);
    try testing.expect(std.mem.eql(u8, r1.?, &utils.int_to_bytes(u8, 2)));

    const r2 = try storage.find(&utils.int_to_bytes(u8, @as(u8, @intCast(5))));
    defer testing.allocator.free(r2.?);
    try testing.expect(std.mem.eql(u8, r2.?, &utils.int_to_bytes(u8, 10)));

    try clean_up(&storage);
}
