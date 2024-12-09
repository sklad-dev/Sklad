const std = @import("std");
const builtin = @import("builtin");

const ArrayList = std.ArrayList;
const AutoHashMap = std.AutoHashMap;

const data_types = @import("./data_types.zig");
const utils = @import("./utils.zig");
const constants = @import("./constants.zig");

const Memtable = @import("./memtable.zig").Memtable;
const SSTable = @import("./sstable.zig").SSTable;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Wal = @import("./wal.zig").Wal;
const StringHashMap = std.StringHashMap;
const StorageRecord = data_types.StorageRecord;

pub fn Storage(comptime V: type) type {
    return struct {
        allocator: std.mem.Allocator,
        path: []const u8,
        max_memtable_size: u16,
        memtable_level_probability: f32,
        memtables: ArrayList(*Memtable(V)),
        table_file_manager: TableFileManager,
        tables: StringHashMap(*SSTable(V)),

        const Self = @This();

        pub fn start(allocator: std.mem.Allocator, path: []const u8, max_memtable_size: u16) !Self {
            var storage = Self{
                .allocator = allocator,
                .path = path,
                .max_memtable_size = max_memtable_size,
                .memtable_level_probability = 0.125,
                .memtables = try ArrayList(*Memtable(V)).initCapacity(allocator, 2),
                .table_file_manager = try TableFileManager.init(allocator, path),
                .tables = StringHashMap(*SSTable(V)).init(allocator),
            };
            try storage.restore_memtables();
            return storage;
        }

        pub inline fn stop(self: *Self) void {
            self.deinit_memtables();
            self.deinit_tables();
            self.table_file_manager.deinit();
        }

        pub fn put(self: *Self, key: []const u8, value: V) !void {
            const record = StorageRecord(V){
                .key_size = @as(u16, @intCast(key.len)),
                .key = key,
                .value = value,
            };

            if (self.memtables.items.len == 0) {
                try self.add_memtable();
            } else if (self.memtables.getLast().size >= self.max_memtable_size) {
                const filled_memtable = self.memtables.pop();

                const max_file_id = self.table_file_manager.level_counters.get(0) orelse -1;
                const file_name_buf = try self.allocator.alloc(u8, 10 + utils.num_digits(i16, max_file_id));
                const file_name = try std.fmt.bufPrint(
                    file_name_buf,
                    "0.{d}.sstable",
                    .{max_file_id + 1},
                );
                var sstable = try SSTable(V).create(
                    self.allocator,
                    filled_memtable,
                    file_name,
                    constants.PAGE_SIZE,
                );
                try self.table_file_manager.add_file(0, file_name_buf);

                sstable.close();
                try filled_memtable.wal.delete_file();
                filled_memtable.destroy();
                self.allocator.destroy(filled_memtable);

                try self.add_memtable();
            }

            const current_memtable = self.memtables.getLast();
            try current_memtable.wal.write(&record);
            try current_memtable.*.add(key, value);
        }

        pub fn find(self: *Self, key: []const u8) !?V {
            var i = self.memtables.items.len;
            while (i > 0) {
                i -= 1;
                if (self.memtables.items[i].find(key)) |value| return value;
            }
            const value = try self.find_in_tables(key);
            return value;
        }

        fn find_in_tables(self: *Self, key: []const u8) !?V {
            var result: ?V = null;
            var result_id: i16 = -1;
            var it = self.table_file_manager.files.iterator();
            while (it.next()) |entry| {
                for (entry.value_ptr.*.items) |file_name| {
                    if (self.tables.contains(file_name) == false) {
                        const table = try self.allocator.create(SSTable(V));
                        table.* = try SSTable(V).open(file_name, self.allocator);
                        try self.tables.put(
                            file_name,
                            table,
                        );
                    }

                    if (try self.tables.get(file_name).?.find(key)) |value| {
                        const file_id = try TableFileManager.parse_file_id(file_name);
                        if (file_id > result_id) {
                            result_id = file_id;
                            result = value;
                        }
                    }
                }
                if (result) |r| {
                    return r;
                }
            }
            return null;
        }

        fn add_memtable(self: *Self) !void {
            const memtable = try self.allocator.create(Memtable(V));
            memtable.* = try Memtable(V).init(
                self.allocator,
                std.crypto.random,
                8,
                self.memtable_level_probability,
            );
            try self.memtables.append(memtable);
        }

        fn restore_memtables(self: *Self) !void {
            var dir = try std.fs.cwd().openDir(self.path, .{
                .access_sub_paths = false,
                .iterate = true,
                .no_follow = true,
            });
            defer dir.close();

            var it = dir.iterate();
            while (try it.next()) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".wal")) {
                    const wal_name = try self.allocator.alloc(u8, 8);
                    @memcpy(wal_name, entry.name);
                    const wal = Wal(V){ .path = wal_name };
                    try self.restore_memtable(wal);
                }
            }
        }

        fn restore_memtable(self: *Self, wal: Wal(V)) !void {
            const memtable = try self.allocator.create(Memtable(V));
            memtable.* = try Memtable(V).from_wal(
                wal,
                self.allocator,
                std.crypto.random,
                8,
                self.memtable_level_probability,
            );

            try self.memtables.append(memtable);
        }

        fn deinit_memtables(self: *Self) void {
            for (self.memtables.items) |t| {
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
}

// Tests
const testing = std.testing;

fn clean_up(comptime V: type, storage: *Storage(V)) !void {
    for (storage.memtables.items) |t| {
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

test "Add value" {
    var test_storage = try Storage(u8).start(testing.allocator, "./", 4);
    defer test_storage.stop();

    try test_storage.put(&utils.key_from_int_data(u8, 1), 42);
    try testing.expect(test_storage.memtables.items.len == 1);
    try testing.expect(test_storage.memtables.getLast().find(&utils.key_from_int_data(u8, 1)) == 42);

    for (test_storage.memtables.items) |t| {
        try t.wal.delete_file();
    }
}

test "Restore memtable from wal" {
    var storage1 = try Storage(u8).start(testing.allocator, "./", 4);
    try storage1.put(&utils.key_from_int_data(u8, 1), 42);
    storage1.stop();

    var storage2 = try Storage(u8).start(testing.allocator, "./", 4);
    defer storage2.stop();

    try testing.expect(storage2.memtables.items.len == 1);
    try testing.expect(storage2.memtables.getLast().find(&utils.key_from_int_data(u8, 1)) == 42);
    for (storage2.memtables.items) |t| {
        try t.wal.delete_file();
    }
}

test "Finding values" {
    var storage = try Storage(u8).start(testing.allocator, "./", 4);
    defer storage.stop();
    try storage.put(&utils.key_from_int_data(u8, 1), 42);

    var search_result = try storage.find(&utils.key_from_int_data(u8, 1));
    try testing.expect(search_result == 42);

    search_result = try storage.find(&utils.key_from_int_data(u8, 2));
    try testing.expect(search_result == null);

    for (2..10) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.key_from_int_data(u8, v), v);
    }

    for (1..10) |i| {
        search_result = try storage.find(&utils.key_from_int_data(u8, @as(u8, @intCast(i))));
        try testing.expect(search_result != null);
    }

    search_result = try storage.find_in_tables(&utils.key_from_int_data(u8, @as(u8, @intCast(1))));
    try testing.expect(search_result != null);
    search_result = try storage.find_in_tables(&utils.key_from_int_data(u8, @as(u8, @intCast(5))));
    try testing.expect(search_result != null);
    search_result = try storage.find_in_tables(&utils.key_from_int_data(u8, @as(u8, @intCast(9))));
    try testing.expect(search_result == null);

    try clean_up(u8, &storage);
}

test "Finding values: return the newest value" {
    var storage = try Storage(u8).start(testing.allocator, "./", 4);
    defer storage.stop();

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.key_from_int_data(u8, v), v);
    }

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.key_from_int_data(u8, v), v * 2);
    }

    const search_result = try storage.find(&utils.key_from_int_data(u8, @as(u8, @intCast(1))));
    try testing.expect(search_result == 2);

    try clean_up(u8, &storage);
}
