const std = @import("std");
const builtin = @import("builtin");

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const AppendDeleteList = @import("./lock_free.zig").AppendDeleteList;
const CompactionState = @import("./table_file_manager.zig").CompactionState;
const Memtable = @import("./memtable.zig").Memtable;
const MergeIterator = @import("./merge_iterator.zig").MergeIterator;
const MetricKind = @import("./metrics.zig").MetricKind;
const SSTable = @import("./sstable.zig").SSTable;
const SSTableCache = @import("./sstable_cache.zig").SSTableCache;
const Handle = @import("./sstable_cache.zig").Handle;
const SSTableIteratorAdapter = @import("./sstable.zig").SSTableIteratorAdapter;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Task = @import("./task_queue.zig").Task;
const Wal = @import("./wal.zig").Wal;

const StorageRecord = data_types.StorageRecord;

const MergeIteratorAdapter = struct {
    merge_iterator: *MergeIterator,

    pub fn init(merge_iterator: *MergeIterator) MergeIteratorAdapter {
        return .{ .merge_iterator = merge_iterator };
    }

    fn nextFn(ctx: *anyopaque) !?StorageRecord {
        const self: *MergeIteratorAdapter = @ptrCast(@alignCast(ctx));
        return self.merge_iterator.next();
    }

    pub fn iterator(self: *MergeIteratorAdapter) StorageRecord.Iterator {
        return .{
            .context = self,
            .next_fn = nextFn,
        };
    }
};

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
            self.submitCompactionTask();
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn submitCompactionTask(self: *FlushTask) void {
            const threshold = global_context.getConfigurator().?.compactionLevelThreshold();
            if (self.storage.table_file_manager.level_counters[0] >= threshold) {
                self.storage.enqueueCompactionTask(0);
            }
        }
    };

    pub const CompactionTask = struct {
        allocator: std.mem.Allocator,
        storage: *BinaryStorage,
        level: u8,

        const CompactionHelper = struct {
            adapters: []SSTableIteratorAdapter,
            handles: []*Handle,
            iterators: []StorageRecord.Iterator,
            records_number: u32,

            fn init(base_task: *CompactionTask, files: *AppendDeleteList([]const u8, []const u8), multiplier: usize) !CompactionHelper {
                var files_tail = try base_task.allocator.alloc([]const u8, multiplier);
                defer base_task.allocator.free(files_tail);

                var files_iterator = files.iterator();
                defer files_iterator.deinit();

                var fi: usize = 0;
                while (files_iterator.next()) |node| : (fi += 1) {
                    files_tail[fi % multiplier] = node.entry.?.*;
                }

                var adapters = try base_task.allocator.alloc(SSTableIteratorAdapter, multiplier);
                errdefer base_task.allocator.free(adapters);

                var handles = try base_task.allocator.alloc(*Handle, multiplier);
                errdefer base_task.allocator.free(handles);

                var iters = try base_task.allocator.alloc(StorageRecord.Iterator, multiplier);
                errdefer base_task.allocator.free(iters);

                var total_records: u32 = 0;
                var i: usize = 0;
                while (i < multiplier) : (i += 1) {
                    handles[i] = try base_task.storage.sstable_cache.get(files_tail[i]);
                    total_records += handles[i].table.records_number;
                    adapters[i] = try SSTableIteratorAdapter.init(handles[i].table);
                    iters[i] = adapters[i].iterator();
                }

                return .{
                    .adapters = adapters,
                    .handles = handles,
                    .iterators = iters,
                    .records_number = total_records,
                };
            }

            pub fn deinit(self: *CompactionHelper, allocator: std.mem.Allocator) void {
                for (0..self.adapters.len) |i| {
                    self.adapters[i].sstable_iterator.deinit();
                    self.handles[i].release();
                }

                allocator.free(self.iterators);
                allocator.free(self.handles);
                allocator.free(self.adapters);
            }
        };

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

        fn do_run(self: *CompactionTask) !void {
            const multiplier = global_context.getConfigurator().?.compactionLevelMultiplier();

            const files = self.storage.table_file_manager.files[self.level];
            if (files == null) return;

            var helper = try CompactionHelper.init(self, files.?, multiplier);

            var merge_iter = try MergeIterator.init(
                self.allocator,
                helper.iterators,
            );
            var adapter = MergeIteratorAdapter.init(&merge_iter);
            var records_merge_iterator = adapter.iterator();
            defer merge_iter.deinit();

            const file_name = try self.storage.table_file_manager.generateFileName(self.level + 1);
            std.log.info("Compaction: creating new sstable file {s}", .{file_name});

            const configurator = global_context.getConfigurator().?;
            var sstable = try SSTable.create(
                self.allocator,
                &records_merge_iterator,
                helper.records_number,
                file_name,
                configurator.sstableBlockSize(),
                configurator.sstableBloomBitsPerKey(),
            );

            self.markOldFilesDeleted(&helper);
            helper.deinit(self.allocator);
            sstable.close(false);

            _ = @atomicRmw(
                u16,
                &self.storage.table_file_manager.level_counters[self.level],
                .Sub,
                @as(u16, @intCast(configurator.compactionLevelMultiplier())),
                .seq_cst,
            );

            _ = @atomicRmw(
                u16,
                &self.storage.table_file_manager.level_counters[self.level + 1],
                .Add,
                1,
                .seq_cst,
            );
            try self.storage.table_file_manager.addFileAtLevel(self.level + 1, file_name);
        }

        fn run(ptr: *anyopaque) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));

            if (@cmpxchgWeak(
                u8,
                &self.storage.table_file_manager.compaction_flags[self.level],
                @intFromEnum(CompactionState.Scheduled),
                @intFromEnum(CompactionState.Running),
                .seq_cst,
                .seq_cst,
            ) != null) {
                return;
            }

            const trace = @errorReturnTrace();
            self.do_run() catch |e| {
                _ = @cmpxchgWeak(
                    u8,
                    &self.storage.table_file_manager.compaction_flags[self.level],
                    @intFromEnum(CompactionState.Running),
                    @intFromEnum(CompactionState.None),
                    .seq_cst,
                    .seq_cst,
                );

                std.log.err("Error! Compaction task at level {d} failed: {any}", .{ self.level, e });
                if (trace) |t| {
                    std.log.err("CompactionTask stack trace:", .{});
                    std.debug.dumpStackTrace(t.*);
                }
                return;
            };

            const configurator = global_context.getConfigurator().?;
            const threshold = configurator.compactionLevelThreshold();
            const max_level = configurator.compactionMaxLevel();

            _ = @cmpxchgWeak(
                u8,
                &self.storage.table_file_manager.compaction_flags[self.level],
                @intFromEnum(CompactionState.Running),
                @intFromEnum(CompactionState.None),
                .seq_cst,
                .seq_cst,
            );

            if (self.level + 1 < max_level and self.storage.table_file_manager.level_counters[self.level + 1] >= threshold) {
                self.storage.enqueueCompactionTask(self.level + 1);
            }
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn file_delete_condition(value: ?*[]const u8, expected: []const u8) bool {
            return value != null and std.mem.eql(u8, value.?.*, expected);
        }

        fn markOldFilesDeleted(self: *CompactionTask, helper: *const CompactionHelper) void {
            const files_ptr = self.storage.table_file_manager.files[self.level];
            if (files_ptr == null) return;
            const files = files_ptr.?;

            for (helper.handles) |handle| {
                files.markDelete(
                    file_delete_condition,
                    handle.table.path,
                );
            }
        }
    };

    pub fn start(allocator: std.mem.Allocator, path: []const u8) !BinaryStorage {
        var table_file_manager = try TableFileManager.init(allocator, path);
        const memtable = try restoreMemtables(&table_file_manager);
        const config = global_context.getConfigurator().?;
        const storage = BinaryStorage{
            .allocator = allocator,
            .path = path,
            .active_memtable = std.atomic.Value(*Memtable).init(memtable),
            .memtables = try AppendDeleteList(KeyedMemtable, u64).init(allocator, cleanUp),
            .table_file_manager = table_file_manager,
            .sstable_cache = try SSTableCache.init(allocator, config.sstableCacheSize()),
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

    pub fn enqueueCompactionTask(self: *BinaryStorage, level: u8) void {
        if (@cmpxchgWeak(
            u8,
            &self.table_file_manager.compaction_flags[level],
            @intFromEnum(CompactionState.None),
            @intFromEnum(CompactionState.Scheduled),
            .seq_cst,
            .seq_cst,
        ) == null) {
            const task_queue = global_context.getTaskQueue();
            var compaction_task = task_queue.?.allocator.create(CompactionTask) catch |e| {
                std.log.err("Error! Failed to create compaction task: {any}", .{e});
                return;
            };

            compaction_task.* = CompactionTask{
                .allocator = self.allocator,
                .storage = self,
                .level = level,
            };

            task_queue.?.enqueue(compaction_task.task());
        }
    }

    fn findInTables(self: *BinaryStorage, key: []const u8) !?[]const u8 {
        var result: ?[]const u8 = null;
        var result_id: u64 = 0;
        const max_level = global_context.getConfigurator().?.compactionMaxLevel();
        for (0..max_level) |level| {
            if (@atomicLoad(?*AppendDeleteList([]const u8, []const u8), &self.table_file_manager.files[level], .seq_cst)) |files| {
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

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

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

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

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

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

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

    try @import("./worker.zig").initWorkerContext(testing.allocator, conf.sstableBlockSize());
    defer @import("./worker.zig").deinitWorkerContext();

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

test "CompactionTask" {
    const block_size: u32 = 64;
    try @import("./worker.zig").initWorkerContext(testing.allocator, block_size);
    defer @import("./worker.zig").deinitWorkerContext();

    const MemtableIteratorAdapter = @import("./sstable.zig").MemtableIteratorAdapter;

    for (0..5) |i| {
        var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 2048, 8, "./");
        defer test_memtable.destroy();

        const test_value = utils.intToBytes(u8, 255);
        var slot: ?Memtable.ReservedDataSlot = null;
        for (0..10) |j| {
            slot = test_memtable.reserve(@sizeOf(usize) + test_value.len);
            try test_memtable.add(&utils.intToBytes(usize, j + i * 10), &test_value, &slot.?);
        }

        var adapter = MemtableIteratorAdapter.init(&test_memtable);
        var memtable_iterator = adapter.iterator();

        const file_name = try std.fmt.allocPrint(testing.allocator, "0.{d}.sstable", .{i});
        defer testing.allocator.free(file_name);

        var test_sstable = try SSTable.create(
            testing.allocator,
            &memtable_iterator,
            test_memtable.size,
            file_name,
            block_size,
            20,
        );
        test_sstable.close(false);
        try test_memtable.wal.deleteFile();
    }

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

    for (0..50) |i| {
        const r = try storage.find(&utils.intToBytes(usize, i));
        defer testing.allocator.free(r.?);
        try testing.expect(std.mem.eql(u8, r.?, &utils.intToBytes(u8, 255)));
    }

    _ = @cmpxchgWeak(
        u8,
        &storage.table_file_manager.compaction_flags[0],
        @intFromEnum(CompactionState.None),
        @intFromEnum(CompactionState.Scheduled),
        .seq_cst,
        .seq_cst,
    );
    var compaction_task = BinaryStorage.CompactionTask{
        .allocator = testing.allocator,
        .storage = &storage,
        .level = 0,
    };
    compaction_task.task().run_fn(&compaction_task);

    for (0..50) |i| {
        const r = try storage.find(&utils.intToBytes(usize, i));
        try testing.expect(std.mem.eql(u8, r.?, &utils.intToBytes(u8, 255)));
        testing.allocator.free(r.?);
    }

    var test_sstable = try SSTable.open(testing.allocator, "./1.0.sstable");
    defer test_sstable.close(true);

    try testing.expect(std.mem.eql(u8, test_sstable.min_key.?, &utils.intToBytes(usize, 0)));
    try testing.expect(std.mem.eql(u8, test_sstable.max_key.?, &utils.intToBytes(usize, 39)));
    try testing.expect(test_sstable.records_number == 40);
    try testing.expect(test_sstable.block_size == block_size);
    try testing.expect(test_sstable.index_records_num == 10);

    try cleanup(&storage);
}
