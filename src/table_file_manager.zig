const std = @import("std");

const global_context = @import("./global_context.zig");
const utils = @import("./utils.zig");

const ArrayList = std.ArrayList;
const AutoHashMap = std.AutoHashMap;
const AppendDeleteList = @import("./lock_free.zig").AppendDeleteList;
const Memtable = @import("./memtable.zig").Memtable;
const SSTable = @import("./sstable.zig").SSTable;

const String = []u8;

pub const TableFileManager = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    files: [256]?*AppendDeleteList(String, String),
    level_counters: [256]u16,

    pub fn string_clean_up(allocator: std.mem.Allocator, value: *String) void {
        allocator.free(value.*);
        allocator.destroy(value);
    }

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !TableFileManager {
        var manager = TableFileManager{
            .allocator = allocator,
            .path = path,
            .files = [_]?*AppendDeleteList(String, String){null} ** 256,
            .level_counters = [_]u16{0} ** 256,
        };
        try manager.mapSstableFiles();
        return manager;
    }

    pub fn deinit(self: *TableFileManager) void {
        self.deinitFiles();
    }

    pub inline fn parseFileId(self: *TableFileManager, file_name: []u8) !u16 {
        const first_dot = std.mem.indexOfScalarPos(u8, file_name, self.path.len, '.').?;
        const second_dot = std.mem.indexOfScalarPos(u8, file_name, first_dot + 1, '.').?;
        return try std.fmt.parseInt(u16, file_name[first_dot + 1 .. second_dot], 10);
    }

    pub fn flushMemtable(self: *TableFileManager, memtable: *Memtable) !void {
        const max_file_id = @atomicRmw(u16, &self.level_counters[0], .Add, 1, .seq_cst);
        const file_name_buf = try self.allocator.alloc(u8, self.path.len + 11 + utils.numDigits(u16, max_file_id));
        const file_name = try std.fmt.bufPrint(
            file_name_buf,
            "{s}/0.{d}.sstable",
            .{ self.path, max_file_id },
        );
        var sstable = try SSTable.create(
            self.allocator,
            memtable,
            file_name,
            global_context.getConfigurator().?.sstableSparseIndexStep(),
        );
        const file_name_ptr = try self.allocator.create([]u8);
        file_name_ptr.* = file_name;
        try self.addFileAtLevel(0, file_name_ptr);

        sstable.close();
        try memtable.wal.deleteFile();
    }

    fn addFileAtLevel(self: *TableFileManager, level: u8, file: *[]u8) !void {
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
        try files_at_level.?.prepend(file);
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
                const file_id: u16 = try std.fmt.parseInt(u16, file_name[first_dot + 1 .. second_dot], 10);
                if (self.level_counters[level_id] < file_id) {
                    self.level_counters[level_id] = file_id;
                }

                const file_name_copy = try self.allocator.alloc(u8, self.path.len + file_name.len);
                _ = try std.fmt.bufPrint(
                    file_name_copy,
                    "{s}{s}",
                    .{ self.path, file_name },
                );
                const file_name_ptr = try self.allocator.create([]u8);
                file_name_ptr.* = file_name_copy;
                try self.addFileAtLevel(level_id, file_name_ptr);
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

fn cleanup(table_manager: *const TableFileManager) void {
    for (0..table_manager.files.len) |level| {
        if (table_manager.files[level]) |files| {
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
        try testing.expect(manager.level_counters[i] == 3);
    }
    try testing.expect(manager.level_counters[4] == 0);

    cleanup(&manager);
    manager.deinit();
}
