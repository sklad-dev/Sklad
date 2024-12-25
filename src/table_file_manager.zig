const std = @import("std");

const ArrayList = std.ArrayList;
const AutoHashMap = std.AutoHashMap;

pub const TableFileManager = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    files: AutoHashMap(u8, *ArrayList([]u8)),
    level_counters: AutoHashMap(u8, i16),

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !TableFileManager {
        var manager = TableFileManager{
            .allocator = allocator,
            .path = path,
            .files = AutoHashMap(u8, *ArrayList([]u8)).init(allocator),
            .level_counters = AutoHashMap(u8, i16).init(allocator),
        };
        try manager.map_sstable_files();
        return manager;
    }

    pub fn deinit(self: *TableFileManager) void {
        self.deinit_files();
        self.deinit_counters();
    }

    pub fn add_file(self: *TableFileManager, level: u8, file: []u8) !void {
        try self.add_file_at_level(level, file);

        const current_level_counter = self.level_counters.get(level) orelse -1;
        try self.level_counters.put(level, current_level_counter + 1);
    }

    pub inline fn parse_file_id(self: *TableFileManager, file_name: []u8) !i16 {
        const first_dot = std.mem.indexOfScalarPos(u8, file_name, self.path.len, '.').?;
        const second_dot = std.mem.indexOfScalarPos(u8, file_name, first_dot + 1, '.').?;
        return try std.fmt.parseInt(i16, file_name[first_dot + 1 .. second_dot], 10);
    }

    fn add_file_at_level(self: *TableFileManager, level: u8, file: []u8) !void {
        if (self.files.contains(level) == false) {
            const level_list = try self.allocator.create(ArrayList([]u8));
            level_list.* = ArrayList([]u8).init(self.allocator);
            try self.files.put(level, level_list);
        }

        const files_at_level = self.files.get(level).?;
        try files_at_level.append(file);
    }

    fn map_sstable_files(self: *TableFileManager) !void {
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
                const file_id: i16 = try std.fmt.parseInt(i16, file_name[first_dot + 1 .. second_dot], 10);
                if (self.level_counters.get(level_id) orelse -1 < file_id) {
                    try self.level_counters.put(level_id, file_id);
                }

                const file_name_copy = try self.allocator.alloc(u8, self.path.len + 1 + file_name.len);
                _ = try std.fmt.bufPrint(
                    file_name_copy,
                    "{s}/{s}",
                    .{ self.path, file_name },
                );
                try self.add_file_at_level(level_id, file_name_copy);
            }
        }
    }

    fn deinit_files(self: *TableFileManager) void {
        var it = self.files.valueIterator();
        while (it.next()) |value| {
            for (value.*.items) |file_name| {
                self.allocator.free(file_name);
            }
            value.*.deinit();
            self.allocator.destroy(value.*);
        }
        self.files.deinit();
    }

    inline fn deinit_counters(self: *TableFileManager) void {
        self.level_counters.deinit();
    }
};

// Tests
const testing = std.testing;

fn create_files() !void {
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

fn clean_up(table_manager: *const TableFileManager) void {
    var it = table_manager.files.iterator();
    while (it.next()) |entry| {
        for (entry.value_ptr.*.items) |file_name| {
            std.fs.cwd().deleteFile(file_name) catch {
                const out = std.io.getStdOut().writer();
                std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
            };
        }
    }
}

test "TableFileManager#init no files" {
    var manager = try TableFileManager.init(testing.allocator, "./");
    defer manager.deinit();

    try testing.expect(manager.files.count() == 0);
    try testing.expect(manager.level_counters.count() == 0);
}

test "TableFileManager#init" {
    // Test that the manager correctly handles existing files
    try create_files();

    var manager = try TableFileManager.init(testing.allocator, "./");
    defer manager.deinit();
    defer clean_up(&manager);

    try testing.expect(manager.files.count() == 4);
    try testing.expect(manager.files.get(0).?.items.len == 4);
    try testing.expect(manager.level_counters.count() == 4);
    try testing.expect(manager.level_counters.get(0) == 3);
}
