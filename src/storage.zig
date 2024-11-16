const std = @import("std");
const builtin = @import("builtin");
const ArrayList = std.ArrayList;
const AutoHashMap = std.AutoHashMap;
const StringHashMap = std.StringHashMap;

const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;
const NodeRecord = data_types.NodeRecord;

const utils = @import("./utils.zig");
const nis = @import("./node_index_storage.zig");
const mt = @import("./memtable.zig");
const st = @import("./sstable.zig");
const w = @import("./wal.zig");
const constants = @import("./constants.zig");

pub fn Storage(comptime N: u8) type {
    return struct {
        const Self = @This();

        path: []const u8,
        max_memtable_size: u16,
        memtable_level_probability: f32,
        allocator: std.mem.Allocator,
        node_index_storage: nis.NodeIndexStorage,
        memtables: ArrayList(*mt.Memtable(N)),
        table_files: AutoHashMap(u8, *ArrayList([]u8)),
        tables: StringHashMap(*st.SSTable),

        pub fn start(path: []const u8, max_memtable_size: u16, allocator: std.mem.Allocator) !Self {
            var node_index_storage = nis.NodeIndexStorage{};
            try node_index_storage.open();

            var storage = Self{
                .path = path,
                .max_memtable_size = max_memtable_size,
                .memtable_level_probability = 0.125,
                .allocator = allocator,
                .node_index_storage = node_index_storage,
                .memtables = try ArrayList(*mt.Memtable(N)).initCapacity(allocator, 2),
                .table_files = AutoHashMap(u8, *ArrayList([]u8)).init(allocator),
                .tables = StringHashMap(*st.SSTable).init(allocator),
            };

            try storage.map_sstable_files();
            try storage.restore_memtables();

            return storage;
        }

        pub fn stop(self: *Self) void {
            self.node_index_storage.close();
            self.deinit_memtables();
            self.deinit_table_files();
            self.deinit_table_levels();
        }

        pub fn write(self: *Self, comptime T: type, value: T) !void {
            const key = utils.key_from_int_data(T, value);
            const key_id = try self.find(&key);
            if (key_id != null) return;

            const node_id = try self.node_index_storage.allocate_next_id();

            const record = try self.allocator.create(NodeRecord);
            record.* = .{
                .node_id = node_id,
                .value_type = try data_types.value_type_from_type(T),
                .value_size = @sizeOf(T),
                .value = &utils.key_from_int_data(T, value),
            };
            defer self.allocator.destroy(record);

            if (self.memtables.items.len == 0) {
                try self.add_memtable();
            } else if (self.memtables.getLast().size >= self.max_memtable_size) {
                const filled_memtable = self.memtables.pop();

                const file_name_buf = try self.allocator.alloc(u8, 14);
                const file_name = try std.fmt.bufPrint(
                    file_name_buf,
                    "0.{s}.sstable",
                    .{filled_memtable.wal_name[0..4]},
                );
                var sstable = try st.SSTable.create(
                    N,
                    filled_memtable,
                    file_name,
                    constants.PAGE_SIZE,
                    self.allocator,
                );
                try self.add_table_file_at_level(0, file_name_buf);

                sstable.close();
                try filled_memtable.wal.delete_file();
                filled_memtable.destroy();
                self.allocator.destroy(filled_memtable);

                try self.add_memtable();
            }

            const current_memtable = self.memtables.getLast();
            try current_memtable.wal.write(record);
            try current_memtable.*.add(
                record.value,
                mt.MemtableValue{
                    .node_id = node_id,
                    .value_type = record.value_type,
                    .value_size = record.value_size,
                },
            );
        }

        pub fn find(self: *Self, key: []const u8) !?u64 {
            for (self.memtables.items) |memtable| {
                const memtable_record = memtable.find(key);
                if (memtable_record != null) {
                    return memtable_record.?.node_id;
                }
            }
            const record_id = try self.find_in_tables(key);
            return record_id;
        }

        fn find_in_tables(self: *Self, key: []const u8) !?u64 {
            var it = self.table_files.iterator();
            while (it.next()) |entry| {
                for (entry.value_ptr.*.items) |file_name| {
                    if (self.tables.contains(file_name) == false) {
                        const table = try self.allocator.create(st.SSTable);
                        table.* = try st.SSTable.open(file_name, self.allocator);
                        try self.tables.put(
                            file_name,
                            table,
                        );
                    }

                    const record_id = try self.tables.get(file_name).?.find(key);
                    if (record_id != null) {
                        return record_id.?;
                    }
                }
            }
            return null;
        }

        fn add_memtable(self: *Self) !void {
            var rng = std.rand.DefaultPrng.init(blk: {
                var seed: u64 = undefined;
                try std.posix.getrandom(std.mem.asBytes(&seed));
                break :blk seed;
            });
            const memtable = try self.allocator.create(mt.Memtable(N));
            memtable.* = try mt.Memtable(N).init(
                self.allocator,
                rng.random(),
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
                    const wal = w.Wal{ .path = wal_name };
                    try self.restore_memtable(wal);
                }
            }
        }

        fn restore_memtable(self: *Self, wal: w.Wal) !void {
            var rng = std.rand.DefaultPrng.init(blk: {
                var seed: u64 = undefined;
                try std.posix.getrandom(std.mem.asBytes(&seed));
                break :blk seed;
            });
            const memtable = try self.allocator.create(mt.Memtable(N));
            memtable.* = try mt.Memtable(N).from_wal(
                wal,
                self.allocator,
                rng.random(),
                self.memtable_level_probability,
            );

            try self.memtables.append(memtable);
        }

        fn add_table_file_at_level(self: *Self, level: u8, table_file: []u8) !void {
            if (self.table_files.contains(level) == false) {
                const level_list = try self.allocator.create(ArrayList([]u8));
                level_list.* = ArrayList([]u8).init(self.allocator);
                try self.table_files.put(level, level_list);
            }
            const level_files = self.table_files.get(level).?;
            try level_files.append(table_file);
        }

        fn map_sstable_files(self: *Self) !void {
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
                    const file_name_copy = try self.allocator.alloc(u8, 14);
                    @memcpy(file_name_copy, file_name);
                    try self.add_table_file_at_level(level_id, file_name_copy);
                }
            }
        }

        fn deinit_memtables(self: *Self) void {
            for (self.memtables.items) |t| {
                t.destroy();
                self.allocator.destroy(t);
            }
            self.memtables.deinit();
        }

        fn deinit_table_files(self: *Self) void {
            var it = self.table_files.valueIterator();
            while (it.next()) |value| {
                for (value.*.items) |file_name| {
                    self.allocator.free(file_name);
                }
                value.*.deinit();
                self.allocator.destroy(value.*);
            }
            self.table_files.deinit();
        }

        fn deinit_table_levels(self: *Self) void {
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

fn delete_index_storage() void {
    std.fs.cwd().deleteFile("./node_index.store") catch {
        const out = std.io.getStdOut().writer();
        std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
    };
}

fn clean_up(comptime N: u8, storage: *Storage(N)) !void {
    delete_index_storage();
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
    defer delete_index_storage();

    var test_storage = try Storage(8).start("./", 4, testing.allocator);
    defer test_storage.stop();

    try test_storage.write(u8, 1);
    try testing.expect(test_storage.memtables.items.len == 1);
    try testing.expect(test_storage.memtables.getLast().find(&utils.key_from_int_data(u8, 1)) != null);

    for (test_storage.memtables.items) |t| {
        try t.wal.delete_file();
    }
}

test "Restore memtable from wal" {
    defer delete_index_storage();

    var storage1 = try Storage(8).start("./", 4, testing.allocator);
    try storage1.write(u8, 1);
    storage1.stop();

    var storage2 = try Storage(8).start("./", 4, testing.allocator);
    defer storage2.stop();

    try testing.expect(storage2.memtables.items.len == 1);
    try testing.expect(storage2.memtables.getLast().find(&utils.key_from_int_data(u8, 1)) != null);
    for (storage2.memtables.items) |t| {
        try t.wal.delete_file();
    }
}

test "Finding values" {
    var storage = try Storage(8).start("./", 4, testing.allocator);
    defer storage.stop();
    try storage.write(u8, 1);

    var search_result = try storage.find(&utils.key_from_int_data(u8, 1));
    try testing.expect(search_result != null);

    search_result = try storage.find(&utils.key_from_int_data(u8, 2));
    try testing.expect(search_result == null);

    for (2..10) |i| {
        try storage.write(u8, @as(u8, @intCast(i)));
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

    try clean_up(8, &storage);
}

test "Add value twice" {
    var test_storage = try Storage(8).start("./", 4, testing.allocator);
    defer test_storage.stop();

    for (1..10) |i| {
        try test_storage.write(u8, @as(u8, @intCast(i)));
    }
    const initial_memtable_size = test_storage.memtables.getLast().size;
    try test_storage.write(u8, @as(u8, @intCast(5)));
    try testing.expect(initial_memtable_size == test_storage.memtables.getLast().size);

    try clean_up(8, &test_storage);
}
