const std = @import("std");
const builtin = @import("builtin");

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const AppendOnlyQueue = @import("./lock_free.zig").AppendOnlyQueue;
const CompactionState = @import("./table_file_manager.zig").CompactionState;
const FileHandle = @import("./sstable.zig").FileHandle;
const Memtable = @import("./memtable.zig").Memtable;
const MergeIterator = @import("./merge_iterator.zig").MergeIterator;
const MetricKind = @import("./metrics.zig").MetricKind;
const RefCounted = @import("./lock_free.zig").RefCounted;
const SSTable = @import("./sstable.zig").SSTable;
const SSTableCache = @import("./sstable_cache.zig").SSTableCache;
const SSTableIteratorAdapter = @import("./sstable.zig").SSTableIteratorAdapter;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Task = @import("./task_queue.zig").Task;
const Wal = @import("./wal.zig").Wal;
const recordMetric = @import("./metrics.zig").recordMetric;

const StorageRecord = data_types.StorageRecord;
const FileList = TableFileManager.FileList;

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
    pending_memtables: std.atomic.Value(?*PendingMemtableList),
    swap_in_progress: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    table_file_manager: TableFileManager,
    sstable_cache: SSTableCache,

    pub const PendingMemtable = struct {
        id: u64,
        memtable: *Memtable,
        flushed: std.atomic.Value(bool),
    };
    pub const PendingMemtableList = RefCounted(AppendOnlyQueue(PendingMemtable, null));

    pub fn pendingMemtableCleanup(allocator: std.mem.Allocator, pm: *PendingMemtable) void {
        pm.memtable.destroy();
        allocator.destroy(pm.memtable);
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

            const list = self.storage.pending_memtables.load(.acquire) orelse return;
            _ = list.acquire();
            defer _ = list.release();

            var memtable: ?*Memtable = null;
            var curr = list.get().head.next;
            while (curr) |node| : (curr = node.next) {
                if (node.entry) |entry| {
                    if (entry.id == self.memtable_key) {
                        memtable = entry.memtable;
                        break;
                    }
                }
            }

            if (memtable == null) return;

            self.storage.table_file_manager.flushMemtable(memtable.?) catch |e| {
                std.log.err("Error! Failed to flush a memtable {s}: {any}", .{ memtable.?.wal.path, e });
                return;
            };

            recordMetric(global_context.getMetricsAggregator(), MetricKind.memtableCounter, 0);

            self.storage.cleanupFlushedMemtables() catch |e| {
                std.log.err("Error! Failed to cleanup flushed memtables: {any}", .{e});
            };

            self.submitCompactionTask();
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *FlushTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn submitCompactionTask(self: *FlushTask) void {
            const threshold = global_context.getConfigurator().?.compactionLevelThreshold();
            const files_count = @atomicLoad(u16, &self.storage.table_file_manager.level_counters[0], .acquire);
            if (files_count >= threshold) {
                self.storage.enqueueCompactionTask(0);
            }
        }
    };

    pub const CompactionTask = struct {
        allocator: std.mem.Allocator,
        storage: *BinaryStorage,
        level: u8,

        const CompactionHelper = struct {
            file_ids: []u64,
            adapters: []SSTableIteratorAdapter,
            sstables: []*SSTableCache.CacheRecord,
            iterators: []StorageRecord.Iterator,
            records_number: u32,

            fn init(base_task: *CompactionTask, files: *AppendOnlyQueue(u64, null), multiplier: usize) !CompactionHelper {
                var file_count: usize = 0;
                var current = files.head.next;
                while (current) |node| : (current = node.next) {
                    if (node.entry) |_| file_count += 1;
                }
                const files_to_compact = @min(file_count, multiplier);

                var files_tail = try base_task.allocator.alloc(u64, files_to_compact);

                var i: usize = 0;
                current = files.head.next;
                while (i < files_to_compact and current != null) : (current = current.?.next) {
                    if (current.?.entry) |entry| {
                        files_tail[i] = entry;
                        i += 1;
                    }
                }

                var adapters = try base_task.allocator.alloc(SSTableIteratorAdapter, files_to_compact);
                errdefer base_task.allocator.free(adapters);

                var sstables = try base_task.allocator.alloc(*SSTableCache.CacheRecord, files_to_compact);
                errdefer base_task.allocator.free(sstables);

                var iters = try base_task.allocator.alloc(StorageRecord.Iterator, files_to_compact);
                errdefer base_task.allocator.free(iters);

                var total_records: u32 = 0;
                for (0..files_to_compact) |j| {
                    sstables[j] = (try base_task.storage.sstable_cache.get(.{
                        .level = base_task.level,
                        .id = files_tail[j],
                    }, &base_task.storage.table_file_manager)).?;
                    total_records += sstables[j].getConst().table.records_number;
                    adapters[j] = try SSTableIteratorAdapter.init(sstables[j].getConst().table);
                    iters[j] = adapters[j].iterator();
                }

                return .{
                    .file_ids = files_tail,
                    .adapters = adapters,
                    .sstables = sstables,
                    .iterators = iters,
                    .records_number = total_records,
                };
            }

            pub fn deinit(self: *CompactionHelper, allocator: std.mem.Allocator) void {
                for (0..self.adapters.len) |i| {
                    self.adapters[i].sstable_iterator.deinit();
                    _ = self.sstables[i].release();
                }

                allocator.free(self.file_ids);
                allocator.free(self.iterators);
                allocator.free(self.sstables);
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

            const files = self.storage.table_file_manager.acquireFilesAtLevel(self.level) orelse return;
            defer _ = files.release();

            var helper = try CompactionHelper.init(self, files.get(), multiplier);
            defer helper.deinit(self.allocator);

            var merge_iter = try MergeIterator.init(
                self.allocator,
                helper.iterators,
            );
            defer merge_iter.deinit();

            var adapter = MergeIteratorAdapter.init(&merge_iter);
            var records_merge_iterator = adapter.iterator();

            const file_id = self.storage.table_file_manager.nextFileIdForLevel(self.level + 1);

            const configurator = global_context.getConfigurator().?;
            var sstable = try SSTable.create(
                self.allocator,
                &records_merge_iterator,
                helper.records_number,
                .{ .level = self.level + 1, .id = file_id },
                configurator.sstableBlockSize(),
                configurator.sstableBloomBitsPerKey(),
            );
            defer sstable.close(false);

            try self.storage.table_file_manager.addFileAtLevel(self.level + 1, sstable.handle.id);
            try self.storage.table_file_manager.deleteFilesAtLevel(self.level, helper.file_ids);
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

            const files_count = @atomicLoad(u16, &self.storage.table_file_manager.level_counters[self.level + 1], .acquire);
            if (self.level + 1 < max_level and files_count >= threshold) {
                self.storage.enqueueCompactionTask(self.level + 1);
            }
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *CompactionTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }
    };

    pub fn start(allocator: std.mem.Allocator, path: []const u8) !BinaryStorage {
        var table_file_manager = try TableFileManager.init(allocator, path);
        const memtable = try restoreMemtables(&table_file_manager);
        const config = global_context.getConfigurator().?;
        var storage = BinaryStorage{
            .allocator = allocator,
            .path = path,
            .active_memtable = std.atomic.Value(*Memtable).init(memtable),
            .pending_memtables = std.atomic.Value(?*PendingMemtableList).init(null),
            .table_file_manager = table_file_manager,
            .sstable_cache = undefined,
        };
        storage.sstable_cache = try SSTableCache.init(allocator, config.sstableCacheSize());
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

                    try self.addPendingMemtable(memtable_key, current);

                    self.active_memtable.store(new_memtable, .release);
                    filled_memtable = current;
                    filled_memtable_key = memtable_key;

                    recordMetric(global_context.getMetricsAggregator(), MetricKind.memtableCounter, 1);
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

        if (self.pending_memtables.load(.acquire)) |list| {
            _ = list.acquire();
            defer _ = list.release();

            var curr = list.get().head.next;
            while (curr) |node| : (curr = node.next) {
                if (node.entry) |entry| {
                    value = entry.memtable.find(key);
                    if (value) |v| {
                        const result = try self.allocator.alloc(u8, v.len);
                        @memcpy(result, v);
                        return result;
                    }
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

    fn addPendingMemtable(self: *BinaryStorage, id: u64, memtable: *Memtable) !void {
        const pending = PendingMemtable{
            .id = id,
            .memtable = memtable,
            .flushed = std.atomic.Value(bool).init(false),
        };

        var old_list = self.pending_memtables.load(.acquire);

        if (old_list == null) {
            const new_list = try self.allocator.create(PendingMemtableList);
            new_list.* = PendingMemtableList.init(self.allocator, AppendOnlyQueue(PendingMemtable, null).init(self.allocator));

            if (self.pending_memtables.cmpxchgStrong(null, new_list, .acq_rel, .acquire)) |existing| {
                new_list.get().deinit();
                self.allocator.destroy(new_list);
                old_list = existing;
            } else {
                old_list = new_list;
            }
        }

        old_list.?.get().enqueue(pending);
    }

    fn findInTables(self: *BinaryStorage, key: []const u8) !?[]const u8 {
        var result: ?[]const u8 = null;
        var result_id: u64 = 0;
        const max_level = global_context.getConfigurator().?.compactionMaxLevel();
        for (0..max_level) |level| {
            var files = self.table_file_manager.acquireFilesAtLevel(@intCast(level)) orelse continue;
            defer _ = files.release();

            var current = files.get().head.next;
            while (current) |node| : (current = node.next) {
                if (node.entry) |file_id| {
                    var cached_record = try self.sstable_cache.get(.{
                        .level = @intCast(level),
                        .id = file_id,
                    }, &self.table_file_manager) orelse continue;
                    defer _ = cached_record.release();

                    if (try cached_record.getConst().table.find(key)) |value| {
                        if (file_id >= result_id) {
                            result_id = file_id;
                            if (result) |r| self.allocator.free(r);
                            result = value;
                        } else {
                            self.allocator.free(value);
                        }
                    }
                }
            }
            if (result) |r| {
                return r;
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

        recordMetric(global_context.getMetricsAggregator(), MetricKind.memtableCounter, 1);

        return memtable;
    }

    fn cleanupFlushedMemtables(self: *BinaryStorage) !void {
        const old_list = self.pending_memtables.load(.acquire) orelse return;
        _ = old_list.acquire();
        defer _ = old_list.release();

        const new_list = try self.allocator.create(PendingMemtableList);
        new_list.* = PendingMemtableList.init(self.allocator, AppendOnlyQueue(PendingMemtable, null).init(self.allocator));
        errdefer {
            new_list.get().deinit();
            self.allocator.destroy(new_list);
        }

        var curr = old_list.get().head.next;
        while (curr) |node| : (curr = node.next) {
            if (node.entry) |entry| {
                if (!entry.flushed.load(.acquire)) {
                    new_list.get().enqueue(entry);
                } else {
                    pendingMemtableCleanup(self.allocator, @constCast(&entry));
                }
            }
        }

        _ = self.pending_memtables.swap(new_list, .acq_rel);
    }

    fn deinitMemtables(self: *BinaryStorage) void {
        const active = self.active_memtable.load(.acquire);
        active.destroy();
        self.allocator.destroy(active);

        if (self.pending_memtables.load(.acquire)) |list| {
            var curr = list.get().head.next;
            while (curr) |node| : (curr = node.next) {
                if (node.entry) |entry| {
                    pendingMemtableCleanup(self.allocator, @constCast(&entry));
                }
            }
            _ = list.release();
        }
    }
};

// Tests
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

fn cleanup(storage: *BinaryStorage) !void {
    try storage.active_memtable.load(.unordered).wal.deleteFile();

    if (storage.pending_memtables.load(.acquire)) |list| {
        _ = list.acquire();
        defer _ = list.release();

        var curr = list.get().head.next;
        while (curr) |node| : (curr = node.next) {
            if (node.entry) |entry| {
                try entry.memtable.wal.deleteFile();
            }
        }
    }

    for (0..storage.table_file_manager.files.len) |level| {
        if (storage.table_file_manager.acquireFilesAtLevel(@intCast(level))) |files| {
            defer _ = files.release();

            var current = files.get().head.next;
            while (current) |node| : (current = node.next) {
                if (node.entry) |file_id| {
                    var cached_record = try storage.sstable_cache.get(.{
                        .level = @intCast(level),
                        .id = file_id,
                    }, &storage.table_file_manager) orelse continue;
                    defer _ = cached_record.release();
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
        if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".sstable")) {
            try std.fs.cwd().deleteFile(entry.name);
        }
    }
}

test "BinaryStorage#put" {
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    defer global_context.deinitConfigurationForTests();

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
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    defer global_context.deinitConfigurationForTests();

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
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

    var configurator = try testing.allocator.create(TestingConfigurator);
    configurator.* = TestingConfigurator.init();
    defer global_context.deinitConfigurationForTests();

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
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

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
    global_context.setRootFolderForTests("./");
    defer global_context.resetRootFolderForTests();

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

        var test_sstable = try SSTable.create(
            testing.allocator,
            &memtable_iterator,
            test_memtable.size,
            .{ .level = 0, .id = @as(u64, i) },
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

    var curr = storage.table_file_manager.acquireFilesAtLevel(0).?.get().head.next;
    while (curr) |node| : (curr = node.next) {
        if (node.entry) |file_id| {
            try testing.expect(file_id != 0);
            try testing.expect(file_id != 1);
            try testing.expect(file_id != 2);
            try testing.expect(file_id != 3);
        }
    }

    for (0..50) |i| {
        const r = try storage.find(&utils.intToBytes(usize, i));
        try testing.expect(std.mem.eql(u8, r.?, &utils.intToBytes(u8, 255)));
        testing.allocator.free(r.?);
    }

    var test_sstable = try SSTable.open(testing.allocator, .{ .level = 1, .id = 0 });
    defer test_sstable.close(true);

    try testing.expect(std.mem.eql(u8, test_sstable.min_key.?, &utils.intToBytes(usize, 0)));
    try testing.expect(std.mem.eql(u8, test_sstable.max_key.?, &utils.intToBytes(usize, 39)));
    try testing.expect(test_sstable.records_number == 40);
    try testing.expect(test_sstable.block_size == block_size);
    try testing.expect(test_sstable.index_records_num == 10);

    try cleanup(&storage);
}
