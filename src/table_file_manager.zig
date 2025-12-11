const std = @import("std");

const fileNameFromHandle = @import("./sstable.zig").fileNameFromHandle;
const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");

const AppendOnlyQueue = @import("./lock_free.zig").AppendOnlyQueue;
const FileHandle = @import("./data_types.zig").FileHandle;
const Manifest = @import("./manifest.zig").Manifest;
const Memtable = @import("./memtable.zig").Memtable;
const MemtableIteratorAdapter = @import("./sstable.zig").MemtableIteratorAdapter;
const Queue = @import("./lock_free.zig").Queue;
const RefCounted = @import("./lock_free.zig").RefCounted;
const SSTable = @import("./sstable.zig").SSTable;
const StorageRecord = @import("./data_types.zig").StorageRecord;

const String = []const u8;

pub const CompactionState = enum(u8) {
    none,
    scheduled,
    running,
};

pub const TableFileManager = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    manifest: Manifest,
    files: [256]std.atomic.Value(?*FileList),
    level_counters: [256]u16,
    max_file_id_per_level: [256]u64,
    compaction_flags: [256]u8,

    pub const FileList = RefCounted(AppendOnlyQueue(u64, null));

    pub fn init(allocator: std.mem.Allocator, path: []const u8, deleted_files: *std.AutoHashMap(FileHandle, void)) !TableFileManager {
        var manager = TableFileManager{
            .allocator = allocator,
            .path = path,
            .manifest = try Manifest.init(allocator, path),
            .files = [_]std.atomic.Value(?*FileList){std.atomic.Value(?*FileList).init(null)} ** 256,
            .level_counters = [_]u16{0} ** 256,
            .max_file_id_per_level = [_]u64{0} ** 256,
            .compaction_flags = [_]u8{0} ** 256,
        };
        try manager.mapSstableFiles(deleted_files);
        return manager;
    }

    pub fn deinit(self: *TableFileManager) void {
        self.manifest.deinit();
        for (0..256) |i| {
            if (self.files[i].load(.acquire)) |file_list| {
                file_list.get().deinit();
                self.allocator.destroy(file_list);
            }
        }
    }

    pub inline fn parseFileId(self: *TableFileManager, file_name: String) !u64 {
        const first_dot = std.mem.indexOfScalarPos(u8, file_name, self.path.len, '.').?;
        const second_dot = std.mem.indexOfScalarPos(u8, file_name, first_dot + 1, '.').?;
        return try std.fmt.parseInt(u64, file_name[first_dot + 1 .. second_dot], 10);
    }

    pub fn nextFileIdForLevel(self: *TableFileManager, level: u8) u64 {
        return @atomicRmw(u64, &self.max_file_id_per_level[level], .Add, 1, .seq_cst);
    }

    pub fn flushMemtable(self: *TableFileManager, memtable: *Memtable) !void {
        const file_id = self.nextFileIdForLevel(0);

        var adapter = MemtableIteratorAdapter.init(memtable);
        var iterator = adapter.iterator();

        const configurator = global_context.getConfigurator().?;
        var sstable = try SSTable.create(
            self.allocator,
            &iterator,
            memtable.size,
            .{ .level = 0, .file_id = file_id },
            configurator.sstableBlockSize(),
            configurator.sstableBloomBitsPerKey(),
        );

        sstable.close(false);
        try self.addFileAtLevel(0, sstable.handle.file_id);
        try memtable.wal.deleteFile();
    }

    pub inline fn acquireFilesAtLevel(self: *TableFileManager, level: u8) ?*FileList {
        const file_list = self.files[level].load(.acquire) orelse return null;
        _ = file_list.acquire();
        return file_list;
    }

    pub fn addFileAtLevel(self: *TableFileManager, level: u8, file_id: u64) !void {
        try self.addFile(level, file_id);

        self.manifest.addFile(level, file_id);
        _ = try self.manifest.flush();
    }

    pub fn deleteFilesAtLevel(self: *TableFileManager, level: u8, file_ids: []const u64) !void {
        var to_delete = std.AutoHashMap(u64, void).init(self.allocator);
        defer to_delete.deinit();
        for (file_ids) |fid| {
            try to_delete.put(fid, {});
        }

        const old_queue = self.files[level].load(.acquire) orelse return;
        _ = old_queue.acquire();
        defer _ = old_queue.release();

        const new_queue = try self.allocator.create(FileList);
        new_queue.* = FileList.init(self.allocator, AppendOnlyQueue(u64, null).init(self.allocator));
        _ = new_queue.acquire();
        defer _ = new_queue.release();

        var deleted_count: u16 = 0;
        var current = old_queue.get().head.next;
        while (current) |node| : (current = node.next) {
            if (node.entry) |file_id| {
                if (!to_delete.contains(file_id)) {
                    new_queue.get().enqueue(file_id);
                } else {
                    deleted_count += 1;
                }
            }
        }

        const swapped_queue = self.files[level].swap(new_queue, .acq_rel);
        if (swapped_queue) |swapped| {
            var seen = std.AutoHashMap(u64, void).init(self.allocator);
            defer seen.deinit();

            var curr_new = new_queue.get().head.next;
            while (curr_new) |node| : (curr_new = node.next) {
                if (node.entry) |fid| {
                    try seen.put(fid, {});
                }
            }

            var curr_swapped = swapped.get().head.next;
            while (curr_swapped) |node| : (curr_swapped = node.next) {
                if (node.entry) |swapped_file_id| {
                    if (!seen.contains(swapped_file_id) and !to_delete.contains(swapped_file_id)) {
                        new_queue.get().enqueue(swapped_file_id);
                    }
                }
            }

            _ = swapped.release();
        }

        _ = @atomicRmw(
            u16,
            &self.level_counters[level],
            .Sub,
            deleted_count,
            .seq_cst,
        );

        for (file_ids) |file_id| {
            self.manifest.removeFile(level, file_id);
        }
        _ = try self.manifest.flush();
    }

    fn addFile(self: *TableFileManager, level: u8, file_id: u64) !void {
        var files_at_level = self.files[level].load(.seq_cst);
        if (files_at_level == null) {
            const level_list = try self.allocator.create(FileList);
            level_list.* = FileList.init(self.allocator, AppendOnlyQueue(u64, null).init(self.allocator));
            const result = self.files[level].cmpxchgWeak(null, level_list, .seq_cst, .seq_cst);
            if (result) |existing| {
                level_list.get().deinit();
                self.allocator.destroy(level_list);
                files_at_level = existing;
            } else {
                files_at_level = level_list;
            }
        }

        _ = @atomicRmw(u16, &self.level_counters[level], .Add, 1, .seq_cst);
        files_at_level.?.get().enqueue(file_id);
    }

    fn mapSstableFiles(self: *TableFileManager, deleted_files: *std.AutoHashMap(FileHandle, void)) !void {
        var dir = try std.fs.cwd().openDir(self.path, .{
            .access_sub_paths = false,
            .iterate = true,
            .no_follow = true,
        });
        defer dir.close();

        try self.manifest.recover(deleted_files);

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".sstable")) {
                const file_name = entry.name;

                const first_dot = std.mem.indexOfScalar(u8, file_name, '.').?; // TODO: handle return value correctly
                const level_id: u8 = try std.fmt.parseInt(u8, file_name[0..first_dot], 10);

                const second_dot = std.mem.indexOfScalarPos(u8, file_name, first_dot + 1, '.').?;
                const file_id: u64 = try std.fmt.parseInt(u64, file_name[first_dot + 1 .. second_dot], 10);
                if (self.max_file_id_per_level[level_id] <= file_id) {
                    self.max_file_id_per_level[level_id] = file_id + 1;
                }

                if (!deleted_files.contains(.{ .level = level_id, .file_id = file_id })) {
                    try self.addFile(level_id, file_id);
                }
            }
        }
    }
};

// Tests
const testing = std.testing;

fn createFiles() !void {
    for (0..4) |i| {
        for (0..4) |j| {
            const file_name_buf = try testing.allocator.alloc(u8, 15);
            defer testing.allocator.free(file_name_buf);

            const file_name = try std.fmt.bufPrint(file_name_buf, "{d}.{d}.sstable", .{ i, j });
            const f = try std.fs.cwd().createFile(file_name, .{
                .read = true,
                .truncate = false,
            });
            f.close();
        }
    }
}

fn cleanup(table_manager: *const TableFileManager) !void {
    for (0..table_manager.files.len) |level| {
        if (table_manager.files[level].load(.unordered)) |files| {
            var curr = files.get().head.next;
            while (curr) |node| : (curr = node.next) {
                const file_name = fileNameFromHandle(
                    testing.allocator,
                    table_manager.path,
                    .{ .level = @intCast(level), .file_id = node.entry.? },
                ) catch unreachable;
                defer testing.allocator.free(file_name);
                try std.fs.cwd().deleteFile(file_name);
            }
        }
    }
}

test "TableFileManager#init" {
    // Test that the manager correctly handles existing files
    try createFiles();

    var deleted_files = std.AutoHashMap(FileHandle, void).init(testing.allocator);
    defer deleted_files.deinit();

    var manager = try TableFileManager.init(testing.allocator, "./", &deleted_files);

    for (0..4) |i| {
        try testing.expect(manager.files[i].load(.unordered) != null);
    }
    try testing.expect(manager.files[4].load(.unordered) == null);

    var list = manager.files[0].load(.unordered).?;
    var counter: usize = 0;
    var curr = list.get().head.next;
    while (curr) |node| : (curr = node.next) {
        counter += 1;
    }
    try testing.expect(counter == 4);

    for (0..4) |i| {
        try testing.expect(manager.max_file_id_per_level[i] == 4);
        try testing.expect(manager.level_counters[i] == 4);
    }
    try testing.expect(manager.max_file_id_per_level[4] == 0);
    try testing.expect(manager.max_file_id_per_level[4] == 0);

    try cleanup(&manager);
    manager.deinit();
}

test "TableFileManager#deleteFilesAtLevel" {
    var deleted_files = std.AutoHashMap(FileHandle, void).init(testing.allocator);
    defer deleted_files.deinit();

    var manager = try TableFileManager.init(testing.allocator, "./", &deleted_files);
    defer manager.deinit();

    try manager.addFileAtLevel(0, 1);
    try manager.addFileAtLevel(0, 2);
    try manager.addFileAtLevel(0, 3);
    try manager.addFileAtLevel(0, 4);

    try testing.expect(manager.level_counters[0] == 4);

    try manager.deleteFilesAtLevel(0, &[_]u64{ 2, 4 });

    const list = manager.acquireFilesAtLevel(0).?;
    _ = list.release();

    var counter: usize = 0;
    var curr = list.get().head.next;
    var seen = std.AutoHashMap(u64, void).init(testing.allocator);
    defer seen.deinit();

    while (curr) |node| : (curr = node.next) {
        counter += 1;
        try seen.put(node.entry.?, {});
    }
    try testing.expect(counter == 2);
    try testing.expect(seen.contains(1));
    try testing.expect(seen.contains(3));
    try testing.expect(manager.level_counters[0] == 2);

    try std.fs.cwd().deleteFile("./MANIFEST");
}
