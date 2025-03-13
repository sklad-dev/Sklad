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
const Memtable = @import("./memtable.zig").Memtable;
const SSTable = @import("./sstable.zig").SSTable;
const TableFileManager = @import("./table_file_manager.zig").TableFileManager;
const Wal = @import("./wal.zig").Wal;

const StorageRecord = data_types.StorageRecord;

pub const BinaryStorage = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    max_memtable_size: u16,
    memtables: ArrayList(*Memtable),
    table_file_manager: TableFileManager,
    tables: StringHashMap(*SSTable),

    const Self = @This();

    pub fn start(allocator: std.mem.Allocator, path: []const u8, max_memtable_size: u16) !Self {
        var storage = Self{
            .allocator = allocator,
            .path = path,
            .max_memtable_size = max_memtable_size,
            .memtables = try ArrayList(*Memtable).initCapacity(allocator, 2),
            .table_file_manager = try TableFileManager.init(allocator, path),
            .tables = StringHashMap(*SSTable).init(allocator),
        };
        try storage.restore_memtables();
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

        if (self.memtables.items.len == 0) {
            try self.add_memtable();
        } else if (self.memtables.getLast().size >= self.max_memtable_size) {
            const filled_memtable = self.memtables.pop().?;

            const max_file_id = self.table_file_manager.level_counters.get(0) orelse -1;
            const file_name_buf = try self.allocator.alloc(u8, self.path.len + 11 + utils.num_digits(i16, max_file_id + 1));
            const file_name = try std.fmt.bufPrint(
                file_name_buf,
                "{s}/0.{d}.sstable",
                .{ self.path, max_file_id + 1 },
            );
            var sstable = try SSTable.create(
                self.allocator,
                filled_memtable,
                file_name,
                global_context.get_configurator().?.sstable_sparse_index_step(),
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
        try current_memtable.add(key, value);
    }

    pub fn find(self: *Self, key: []const u8) !?[]const u8 {
        var i = self.memtables.items.len;
        while (i > 0) {
            i -= 1;
            const value = try self.memtables.items[i].find(key);
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

    fn add_memtable(self: *Self) !void {
        const config = global_context.get_configurator().?;
        const memtable = try self.allocator.create(Memtable);
        memtable.* = try Memtable.init(
            self.allocator,
            std.crypto.random,
            config.memtable_max_level(),
            config.memtable_level_probability(),
            self.path,
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
                const wal_name = try self.allocator.alloc(u8, self.path.len + 9);
                const wal = try Wal.open(
                    self.allocator,
                    try std.fmt.bufPrint(
                        wal_name,
                        "{s}/{s}",
                        .{ self.path, entry.name },
                    ),
                );
                try self.restore_memtable(wal);
            }
        }
    }

    fn restore_memtable(self: *Self, wal: Wal) !void {
        const config = global_context.get_configurator().?;
        const memtable = try self.allocator.create(Memtable);
        memtable.* = try Memtable.from_wal(
            wal,
            self.allocator,
            std.crypto.random,
            config.memtable_max_level(),
            config.memtable_level_probability(),
        );

        try self.memtables.append(memtable);
    }

    fn deinit_memtables(self: *const Self) void {
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

// Tests
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;

fn clean_up(storage: *BinaryStorage) !void {
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
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var test_storage = try BinaryStorage.start(testing.allocator, "./", 4);
    defer test_storage.stop();

    try test_storage.put(&utils.int_to_bytes(u8, 1), &utils.int_to_bytes(u8, 42));
    try testing.expect(test_storage.memtables.items.len == 1);
    const result = try test_storage.memtables.getLast().find(&utils.int_to_bytes(u8, 1));
    try testing.expect(std.mem.eql(u8, result.?, &utils.int_to_bytes(u8, 42)));

    for (test_storage.memtables.items) |t| {
        try t.wal.delete_file();
    }
}

test "Restore memtable from wal" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var storage1 = try BinaryStorage.start(testing.allocator, "./", 4);
    try storage1.put(&utils.int_to_bytes(u8, 1), &utils.int_to_bytes(u8, 42));
    storage1.stop();

    var storage2 = try BinaryStorage.start(testing.allocator, "./", 4);
    defer storage2.stop();

    try testing.expect(storage2.memtables.items.len == 1);
    const result = try storage2.memtables.getLast().find(&utils.int_to_bytes(u8, 1));
    try testing.expect(std.mem.eql(u8, result.?, &utils.int_to_bytes(u8, 42)));
    for (storage2.memtables.items) |t| {
        try t.wal.delete_file();
    }
}

test "Finding values" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var storage = try BinaryStorage.start(testing.allocator, "./", 4);
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

    search_result = try storage.find_in_tables(&utils.int_to_bytes(u8, @as(u8, @intCast(1))));
    try testing.expect(search_result != null);
    testing.allocator.free(search_result.?);

    search_result = try storage.find_in_tables(&utils.int_to_bytes(u8, @as(u8, @intCast(5))));
    try testing.expect(search_result != null);
    testing.allocator.free(search_result.?);

    search_result = try storage.find_in_tables(&utils.int_to_bytes(u8, @as(u8, @intCast(9))));
    try testing.expect(search_result == null);

    try clean_up(&storage);
}

test "Finding values: return the newest value" {
    var configurator = try testing.allocator.create(TestingConfigurator);
    defer global_context.deinit_configuration_for_tests();

    configurator.* = TestingConfigurator.init();
    var conf = configurator.configurator();
    global_context.load_configuration(&conf);

    var storage = try BinaryStorage.start(testing.allocator, "./", 4);
    defer storage.stop();

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.int_to_bytes(u8, v), &utils.int_to_bytes(u8, v));
    }

    for (0..8) |i| {
        const v = @as(u8, @intCast(i));
        try storage.put(&utils.int_to_bytes(u8, v), &utils.int_to_bytes(u8, v * 2));
    }

    const search_result = try storage.find(&utils.int_to_bytes(u8, @as(u8, @intCast(1))));
    try testing.expect(std.mem.eql(u8, search_result.?, &utils.int_to_bytes(u8, 2)));
    testing.allocator.free(search_result.?);

    try clean_up(&storage);
}
