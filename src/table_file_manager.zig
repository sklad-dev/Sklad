const std = @import("std");

const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");

const AppendDeleteList = @import("./lock_free.zig").AppendDeleteList;
const Memtable = @import("./memtable.zig").Memtable;
const SSTable = @import("./sstable.zig").SSTable;
const MemtableIteratorAdapter = @import("./sstable.zig").MemtableIteratorAdapter;
const StorageRecord = @import("./data_types.zig").StorageRecord;

const String = []const u8;

pub const CompactionState = enum(u8) {
    None,
    Scheduled,
    Running,
};

pub const TableFileManager = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    files: [256]?*AppendDeleteList(String, String),
    level_counters: [256]u16,
    max_file_id_per_level: [256]u64,
    compaction_flags: [256]u8,

    pub fn string_clean_up(allocator: std.mem.Allocator, value: *String) void {
        std.fs.cwd().deleteFile(value.*) catch |e| {
            std.log.err("Failed to delete file {s}: {any}\n", .{ value.*, e });
        };
        allocator.free(value.*);
        allocator.destroy(value);
    }

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !TableFileManager {
        var manager = TableFileManager{
            .allocator = allocator,
            .path = path,
            .files = [_]?*AppendDeleteList(String, String){null} ** 256,
            .level_counters = [_]u16{0} ** 256,
            .max_file_id_per_level = [_]u64{0} ** 256,
            .compaction_flags = [_]u8{0} ** 256,
        };
        try manager.mapSstableFiles();
        return manager;
    }

    pub fn deinit(self: *TableFileManager) void {
        self.deinitFiles();
    }

    pub inline fn parseFileId(self: *TableFileManager, file_name: String) !u64 {
        const first_dot = std.mem.indexOfScalarPos(u8, file_name, self.path.len, '.').?;
        const second_dot = std.mem.indexOfScalarPos(u8, file_name, first_dot + 1, '.').?;
        return try std.fmt.parseInt(u64, file_name[first_dot + 1 .. second_dot], 10);
    }

    pub fn generateFileName(self: *TableFileManager, level: u8) ![]u8 {
        const next_id = @atomicRmw(
            u64,
            &self.max_file_id_per_level[level],
            .Add,
            1,
            .seq_cst,
        );
        errdefer _ = @atomicRmw(
            u64,
            &self.max_file_id_per_level[level],
            .Sub,
            1,
            .seq_cst,
        );

        const buf_size = 10 + self.path.len + utils.numDigits(u8, level) + utils.numDigits(u64, next_id);
        const buf = try self.allocator.alloc(u8, buf_size);
        const file_name = try std.fmt.bufPrint(
            buf,
            "{s}/{d}.{d}.sstable",
            .{ self.path, level, next_id },
        );

        return file_name;
    }

    pub fn flushMemtable(self: *TableFileManager, memtable: *Memtable) !void {
        const file_name = try self.generateFileName(0);

        var adapter = MemtableIteratorAdapter.init(memtable);
        var iterator = adapter.iterator();

        const configurator = global_context.getConfigurator().?;
        var sstable = try SSTable.create(
            self.allocator,
            &iterator,
            memtable.size,
            file_name,
            configurator.sstableBlockSize(),
            configurator.sstableBloomBitsPerKey(),
        );

        _ = @atomicRmw(
            u16,
            &self.level_counters[0],
            .Add,
            1,
            .seq_cst,
        );
        try self.addFileAtLevel(0, file_name);

        sstable.close(false);
        try memtable.wal.deleteFile();
    }

    pub fn addFileAtLevel(self: *TableFileManager, level: u8, file: []const u8) !void {
        const owned_file = try self.allocator.create([]const u8);
        owned_file.* = file;

        var files_at_level = @atomicLoad(?*AppendDeleteList(String, String), &self.files[level], .seq_cst);
        if (files_at_level == null) {
            const level_list = try self.allocator.create(AppendDeleteList(String, String));
            level_list.* = try AppendDeleteList(String, String).init(self.allocator, string_clean_up);
            if (@cmpxchgWeak(?*AppendDeleteList(String, String), &self.files[level], null, level_list, .seq_cst, .seq_cst) != null) {
                level_list.deinit();
                self.allocator.destroy(level_list);
            }
        }
        files_at_level = @atomicLoad(?*AppendDeleteList(String, String), &self.files[level], .seq_cst);
        try files_at_level.?.prepend(owned_file);
    }

    fn mapSstableFiles(self: *TableFileManager) !void {
        var dir = try std.fs.cwd().openDir(self.path, .{
            .access_sub_paths = false,
            .iterate = true,
            .no_follow = true,
        });
        defer dir.close();

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

                const file_name_copy = try self.allocator.alloc(u8, self.path.len + file_name.len + 1);
                _ = try std.fmt.bufPrint(
                    file_name_copy,
                    "{s}/{s}",
                    .{ self.path, file_name },
                );
                self.level_counters[level_id] += 1;
                try self.addFileAtLevel(level_id, file_name_copy);
            }
        }
    }

    fn deinitFiles(self: *TableFileManager) void {
        for (0..256) |i| {
            if (self.files[i]) |file_list| {
                file_list.deinit();
                self.allocator.destroy(file_list);
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
        if (table_manager.files[level]) |files| {
            var it = files.iterator();
            defer it.deinit();
            while (it.next()) |node| {
                const file_name = node.entry.?.*;
                try std.fs.cwd().deleteFile(file_name);
            }
        }
    }
}

test "TableFileManager#init" {
    // Test that the manager correctly handles existing files
    try createFiles();

    var manager = try TableFileManager.init(testing.allocator, "./");

    for (0..4) |i| {
        try testing.expect(manager.files[i] != null);
    }
    try testing.expect(manager.files[4] == null);

    var it = manager.files[0].?.iterator();
    var counter: usize = 0;
    while (it.next() != null) {
        counter += 1;
    }
    it.deinit();
    try testing.expect(counter == 4);

    for (0..4) |i| {
        try testing.expect(manager.max_file_id_per_level[i] == 4);
        try testing.expect(manager.level_counters[i] == 4);
    }
    try testing.expect(manager.max_file_id_per_level[4] == 0);
    try testing.expect(manager.max_file_id_per_level[4] == 0);

    // try cleanup(&manager);
    manager.deinit();
}
