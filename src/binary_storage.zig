const std = @import("std");
const builtin = @import("builtin");

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const AppendDeleteList = @import("./lock_free.zig").AppendDeleteList;
const LoserTreeIterator = @import("./loser_tree.zig").LoserTreeIterator;
const Memtable = @import("./memtable.zig").Memtable;
const MetricKind = @import("./metrics.zig").MetricKind;
const SSTable = @import("./sstable.zig").SSTable;
const SSTableCache = @import("./sstable_cache.zig").SSTableCache;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Task = @import("./task_queue.zig").Task;
const Wal = @import("./wal.zig").Wal;

const StorageRecord = data_types.StorageRecord;

pub const BinaryStorage = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    active_memtable: std.atomic.Value(*Memtable),
    memtables: AppendDeleteList(KeyedMemtable, u64),
    swap_in_progress: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    table_file_manager: TableFileManager,
    sstable_cache: SSTableCache,

    const KeyedMemtable = struct {
        id: u64,
        memtable: *Memtable,
    };

    pub fn cleanUp(allocator: std.mem.Allocator, keyed_memtable: *KeyedMemtable) void {
        keyed_memtable.memtable.destroy();
        allocator.destroy(keyed_memtable.memtable);
        allocator.destroy(keyed_memtable);
    }

    fn condition(value: ?*KeyedMemtable, expected: u64) bool {
        return value != null and value.?.id == expected;
    }

    pub const FlushTask = struct {
        allocator: std.mem.Allocator,
        memtable_key: u64,
        storage: *BinaryStorage,

        pub fn init(allocator: std.mem.Allocator, memtable_key: u64, storage: *BinaryStorage) !FlushTask {
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
                .enqued_at = std.time.microTimestamp(),
            };
        }

        fn run(ptr: *anyopaque) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));

            var memtable: *Memtable = undefined;
            {
                var iter = self.storage.memtables.iterator();
                defer iter.deinit();
                while (iter.next()) |node| {
                    if (node.entry.?.id == self.memtable_key) {
                        memtable = node.entry.?.memtable;
                        break;
                    }
                }
            }

            self.storage.table_file_manager.flushMemtable(memtable) catch |e| {
                std.log.err("Error! Failed to falush a memtable {s}: {any}", .{ memtable.wal.path, e });
                return;
            };

            _ = global_context.getMetricsAggregator().?.record(.{
                .timestamp = std.time.microTimestamp(),
                .value = 1,
                .kind = @intFromEnum(MetricKind.memtableCounter),
            });

            self.storage.memtables.markDelete(condition, self.memtable_key);
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }
    };

    pub const CompactionTask = struct {
        allocator: std.mem.Allocator,
        storage: *BinaryStorage,
        level: u8,

        pub fn init(allocator: std.mem.Allocator, storage: *BinaryStorage, level: u8) !CompactionTask {
            return .{
                .allocator = allocator,
                .storage = storage,
                .level = level,
            };
        }

        pub fn task(self: *CompactionTask) Task {
            return .{
                .context = self,
                .run_fn = CompactionTask.run,
                .destroy_fn = CompactionTask.destroy,
                .enqued_at = std.time.microTimestamp(),
            };
        }

        fn run(ptr: *anyopaque) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));
            const multiplier = global_context.getConfiguration().compactionLevelMultiplier();

            var files = self.storage.table_file_manager.files[self.level];
            if (files == null) return;

            var tail_files = try self.allocator.alloc([]u8, multiplier);
            defer self.allocator.free(tail_files);

            for (files.?.iterator(), 0..) |node, i| {
                tail_files[i % multiplier] = node.entry.?.*;
            }

            var iterators: [*]LoserTreeIterator(StorageRecord).SourceIterator = try self.allocator.alloc([]u8, multiplier);
            defer self.allocator.free(iterators);

            for (tail_files, 0..) |file_name, i| {
                const sstable_file = try SSTable.open(self.allocator, file_name);
                iterators[i] = sstable_file.iterator();
            }

            var loser_tree_iter = try LoserTreeIterator(StorageRecord).init(
                self.allocator,
                &iterators,
                compareRecords,
            );
            defer loser_tree_iter.deinit();
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn compareRecords(a: StorageRecord, b: StorageRecord) i8 {
            return utils.compareBitwise(a.key, b.key);
        }
    };

    pub fn start(allocator: std.mem.Allocator, path: []const u8) !BinaryStorage {
        var table_file_manager = try TableFileManager.init(allocator, path);
        const memtable = try restoreMemtables(&table_file_manager);
        const storage = BinaryStorage{
            .allocator = allocator,
            .path = path,
            .active_memtable = std.atomic.Value(*Memtable).init(memtable),
            .memtables = try AppendDeleteList(KeyedMemtable, u64).init(allocator, cleanUp),
            .table_file_manager = table_file_manager,
            .sstable_cache = try SSTableCache.init(allocator, 8),
        };
        return storage;
    }

    pub inline fn stop(self: *BinaryStorage) void {
        self.deinitMemtables();
        self.table_file_manager.deinit();
        self.sstable_cache.deinit();
    }

    pub fn put(self: *BinaryStorage, key: []const u8, value: []const u8) !void {
        const record = StorageRecord{
            .key = key,
            .value = value,
        };

        var filled_memtable: ?*Memtable = null;
        var filled_memtable_key: u64 = undefined;
        var memtable: *Memtable = undefined;
        var data_slot: ?Memtable.ReservedDataSlot = null;

        const payload_size: u64 = @intCast(key.len + value.len);

        while (true) {
            memtable = self.active_memtable.load(.acquire);

            data_slot = memtable.reserve(payload_size);
            if (data_slot) |_| break;

            const was_swapping = self.swap_in_progress.swap(true, .acq_rel);
            if (!was_swapping) {
                const current = self.active_memtable.load(.acquire);
                if (current == memtable and memtable.reserve(payload_size) == null) {
                    const new_memtable = try Memtable.create(self.allocator, self.path);
                    const memtable_key: u64 = std.crypto.random.int(u64);
                    const keyed = try self.allocator.create(KeyedMemtable);
                    keyed.* = .{
                        .id = memtable_key,
                        .memtable = current,
                    };
                    try self.memtables.prepend(keyed); // TODO: handle failure to prepend

                    self.active_memtable.store(new_memtable, .release);
                    filled_memtable = current;
                    filled_memtable_key = memtable_key;
                }
                self.swap_in_progress.store(false, .release);
                continue;
            } else {
                std.atomic.spinLoopHint();
                continue;
            }
        }

        try memtable.wal.writeRecord(&record);
        try memtable.add(key, value, &(data_slot.?));

        if (filled_memtable) |_| {
            const task_queue = global_context.getTaskQueue();
            var flush_task = try task_queue.?.allocator.create(FlushTask);

            flush_task.* = FlushTask{
                .allocator = self.allocator,
                .memtable_key = filled_memtable_key,
                .storage = self,
            };

            task_queue.?.enqueue(flush_task.task());
        }
    }

    pub fn find(self: *BinaryStorage, key: []const u8) !?[]const u8 {
        const active = self.active_memtable.load(.acquire);

        var value = active.find(key);
        if (value) |v| {
            const result = try self.allocator.alloc(u8, v.len);
            @memcpy(result, v);
            return result;
        }

        {
            var iter = self.memtables.iterator();
            defer iter.deinit();
            while (iter.next()) |n| {
                value = n.entry.?.memtable.find(key);
                if (value) |v| {
                    const result = try self.allocator.alloc(u8, v.len);
                    @memcpy(result, v);
                    return result;
                }
            }
        }

        return try self.findInTables(key);
    }

    fn findInTables(self: *BinaryStorage, key: []const u8) !?[]const u8 {
        var result: ?[]const u8 = null;
        var result_id: u16 = 0;
        for (0..self.table_file_manager.files.len) |level| {
            if (@atomicLoad(?*AppendDeleteList([]u8, []u8), &self.table_file_manager.files[level], .seq_cst)) |files| {
                var it = files.iterator();
                defer it.deinit();
                while (it.next()) |node| {
                    const file_name = node.entry.?.*;
                    var handle = try self.sstable_cache.get(file_name);
                    defer handle.release();

                    if (try handle.table.find(key)) |value| {
                        const file_id = try self.table_file_manager.parseFileId(file_name);
                        if (file_id >= result_id) {
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
            } else {
                break;
            }
        }
        return null;
    }

    fn restoreMemtables(table_file_manager: *TableFileManager) !*Memtable {
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
                while (try Memtable.fromWal(wal, memtable) == true) {
                    try table_file_manager.flushMemtable(memtable);
                    memtable.destroy();
                    table_file_manager.allocator.destroy(memtable);
                    memtable = try Memtable.create(table_file_manager.allocator, table_file_manager.path);
                }
                wal.file.close();
                try wal.deleteFile();
                wal.allocator.free(wal.path);
            }
        }

        return memtable;
    }

    fn deinitMemtables(self: *BinaryStorage) void {
        const active = self.active_memtable.load(.acquire);
        active.destroy();
        self.allocator.destroy(active);
        self.memtables.deinit();
    }
};

// Tests
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

fn cleanup(storage: *BinaryStorage) !void {
    try storage.active_memtable.load(.unordered).wal.deleteFile();

    var iter = storage.memtables.iterator();
    defer iter.deinit();

    while (iter.next()) |node| {
        try node.entry.?.memtable.wal.deleteFile();
    }

    for (0..storage.table_file_manager.files.len) |level| {
        if (storage.table_file_manager.files[level]) |files| {
            var it = files.iterator();
            defer it.deinit();
            while (it.next()) |node| {
                const file_name = node.entry.?.*;
                try std.fs.cwd().deleteFile(file_name);
            }
        }
    }
}

test "BinaryStorage#put" {
    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var test_storage = try BinaryStorage.start(testing.allocator, ".");
    defer test_storage.stop();

    try test_storage.put(&utils.intToBytes(u8, 1), &utils.intToBytes(u8, 42));
    try testing.expect(test_storage.active_memtable.load(.unordered).size == 1);
    const result = test_storage.active_memtable.load(.unordered).find(&utils.intToBytes(u8, 1));
    try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u8, 42)));

    try test_storage.active_memtable.load(.unordered).wal.deleteFile();
}

test "Restore memtable from wal" {
    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var storage1 = try BinaryStorage.start(testing.allocator, ".");
    try storage1.put(&utils.intToBytes(u8, 3), &utils.intToBytes(u16, 0xCFCF));
    try storage1.put(&utils.intToBytes(u8, 4), &utils.intToBytes(u16, 0xFAFA));
    storage1.stop();

    var storage2 = try BinaryStorage.start(testing.allocator, ".");
    defer storage2.stop();

    try testing.expect(storage2.active_memtable.load(.unordered).size == 2);

    var result = storage2.active_memtable.load(.unordered).find(&utils.intToBytes(u8, 3));
    try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u16, 0xCFCF)));

    result = storage2.active_memtable.load(.unordered).find(&utils.intToBytes(u8, 4));
    try testing.expect(std.mem.eql(u8, result.?, &utils.intToBytes(u16, 0xFAFA)));

    try storage2.active_memtable.load(.unordered).wal.deleteFile();
}

test "BinaryStorage#find" {
    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    const value = utils.intToBytes(u8, 42);
    try storage.put(&utils.intToBytes(u8, 1), &value);

    var search_result = try storage.find(&utils.intToBytes(u8, 1));
    try testing.expect(std.mem.eql(u8, search_result.?, &value));
    testing.allocator.free(search_result.?);

    search_result = try storage.find(&utils.intToBytes(u8, 2));
    try testing.expect(search_result == null);

    for (2..10) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v));
    }

    for (1..10) |i| {
        search_result = try storage.find(&utils.intToBytes(u8, @as(u8, @intCast(i))));
        defer testing.allocator.free(search_result.?);
        try testing.expect(search_result != null);
    }

    try cleanup(&storage);
}

test "BinaryStorage#find returns the newest value" {
    defer global_context.deinitConfigurationForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.loadConfiguration(&conf);

    var task_queue = TaskQueue.init(testing.allocator);
    global_context.initTaskQueueForTests(&task_queue);
    defer global_context.cleanAndDeinitTaskQueueForTests();

    var storage = try BinaryStorage.start(testing.allocator, ".");
    defer storage.stop();

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v));
    }

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.intToBytes(u8, v), &utils.intToBytes(u8, v * 2));
    }

    const r1 = try storage.find(&utils.intToBytes(u8, @as(u8, @intCast(1))));
    defer testing.allocator.free(r1.?);
    try testing.expect(std.mem.eql(u8, r1.?, &utils.intToBytes(u8, 2)));

    const r2 = try storage.find(&utils.intToBytes(u8, @as(u8, @intCast(5))));
    defer testing.allocator.free(r2.?);
    try testing.expect(std.mem.eql(u8, r2.?, &utils.intToBytes(u8, 10)));

    try cleanup(&storage);
}
