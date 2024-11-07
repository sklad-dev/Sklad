const std = @import("std");
const builtin = @import("builtin");
const ArrayList = std.ArrayList;
const AutoHashMap = std.AutoHashMap;

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
        memtables: ?ArrayList(*mt.Memtable(N)),
        table_files: ?AutoHashMap(u8, ArrayList([]u8)),
        table_levels: ?AutoHashMap(u8, ArrayList(*st.SSTable)),

        pub fn start(path: []const u8, max_memtable_size: u16, allocator: std.mem.Allocator) !Self {
            var node_index_storage = nis.NodeIndexStorage{};
            try node_index_storage.open();

            var storage = Self{
                .path = path,
                .max_memtable_size = max_memtable_size,
                .memtable_level_probability = 0.125,
                .allocator = allocator,
                .node_index_storage = node_index_storage,
                .memtables = null,
                .table_files = null,
                .table_levels = null,
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
            const node_id = try self.node_index_storage.allocate_next_id();

            const record = try self.allocator.create(NodeRecord);
            record.* = .{
                .node_id = node_id,
                .value_type = try value_type_from_type(T),
                .value_size = @sizeOf(T),
                .value = &utils.key_from_int_data(T, value),
            };
            defer self.allocator.destroy(record);

            if (self.memtables == null) {
                self.memtables = try ArrayList(*mt.Memtable(N)).initCapacity(self.allocator, 2);
                try self.add_memtable();
            } else if (self.memtables.?.getLast().size >= self.max_memtable_size) {
                const filled_memtable = self.memtables.?.pop();
                const wal_id = utils.generate_id(filled_memtable.rng);

                var file_name_buf: [24]u8 = undefined;
                const file_name = try std.fmt.bufPrint(
                    &file_name_buf,
                    "0.{x:0>2}{x:0>2}.sstable",
                    .{ wal_id[0], wal_id[1] },
                );
                const sstable = try st.SSTable.create(
                    N,
                    filled_memtable,
                    file_name,
                    constants.PAGE_SIZE,
                    self.allocator,
                );
                sstable.close();
                try filled_memtable.wal.delete_file();
                filled_memtable.destroy();
                self.allocator.destroy(filled_memtable);

                try self.add_memtable();
            }

            const current_memtable = self.memtables.?.getLast();
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

        pub fn find() void {}

        pub fn delete() void {}

        const DataError = error{UnknownType};

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
            try self.memtables.?.append(memtable);
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

            if (self.memtables == null) {
                self.memtables = try ArrayList(*mt.Memtable(N)).initCapacity(self.allocator, 2);
            }
            try self.memtables.?.append(memtable);
        }

        fn value_type_from_type(T: type) !ValueType {
            return switch (T) {
                bool => ValueType.boolean,
                i8 => ValueType.smallint,
                i16, i32 => ValueType.int,
                i64 => ValueType.bigint,
                u8 => ValueType.smallserial,
                u16, u32 => ValueType.serial,
                u64 => ValueType.bigserial,
                f32 => ValueType.float,
                f64 => ValueType.bigfloat,
                else => error.DataError,
            };
        }

        fn map_sstable_files(self: *Self) !void {
            if (self.table_files == null) {
                self.table_files = AutoHashMap(u8, ArrayList([]u8)).init(self.allocator);
            }

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
                    if (self.table_files.?.contains(level_id) == false) {
                        try self.table_files.?.put(level_id, ArrayList([]u8).init(self.allocator));
                    }
                    var level_files = self.table_files.?.get(level_id).?;
                    try level_files.append(file_name_copy);
                }
            }
        }

        fn deinit_memtables(self: *Self) void {
            if (self.memtables) |ts| {
                for (ts.items) |t| {
                    t.destroy();
                    self.allocator.destroy(t);
                }
                ts.deinit();
                self.memtables = null;
            }
        }

        fn deinit_table_files(self: *Self) void {
            if (self.table_files) |table_files| {
                var it = table_files.valueIterator();
                while (it.next()) |value| {
                    for (value.*.items) |file_name| {
                        self.allocator.free(file_name);
                    }
                    value.deinit();
                }
                self.table_files.?.deinit();
                self.table_files = null;
            }
        }

        fn deinit_table_levels(self: *Self) void {
            if (self.table_levels) |table_levels| {
                var it = table_levels.valueIterator();
                while (it.next()) |level| {
                    for (level.*.items) |sstable| {
                        sstable.close();
                    }
                    level.deinit();
                }
                self.table_levels.?.deinit();
                self.table_levels = null;
            }
        }
    };
}

// Tests
const testing = std.testing;

test "Add value" {
    var test_storage = try Storage(8).start("./", 4, testing.allocator);
    try test_storage.write(u8, 1);
    try testing.expect(test_storage.memtables.?.items.len == 1);
    try testing.expect(test_storage.memtables.?.getLast().find(&utils.key_from_int_data(u8, 1)) != null);

    for (test_storage.memtables.?.items) |t| {
        try t.wal.delete_file();
    }
    test_storage.stop();
}

test "Restore memtable from wal" {
    var storage1 = try Storage(8).start("./", 4, testing.allocator);
    try storage1.write(u8, 1);
    storage1.stop();

    var storage2 = try Storage(8).start("./", 4, testing.allocator);
    try testing.expect(storage2.memtables.?.items.len == 1);
    try testing.expect(storage2.memtables.?.getLast().find(&utils.key_from_int_data(u8, 1)) != null);
    for (storage2.memtables.?.items) |t| {
        try t.wal.delete_file();
    }
    storage2.stop();
}
