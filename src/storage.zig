const std = @import("std");
const builtin = @import("builtin");
const ArrayList = std.ArrayList;

const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;
const NodeRecord = data_types.NodeRecord;

const w = @import("./wal.zig");
const nis = @import("./node_index_storage.zig");
const mt = @import("./memtable.zig");
const st = @import("./sstable.zig");

pub fn Storage(comptime N: u8) type {
    return struct {
        const Self = @This();

        path: []const u8,
        max_memtable_size: u16,
        memtable_level_probability: f32,
        allocator: std.mem.Allocator,
        wal: w.Wal,
        node_index_storage: nis.NodeIndexStorage,
        memtables: ?ArrayList(*mt.Memtable(N)),

        pub fn init(path: []const u8, max_memtable_size: u16, allocator: std.mem.Allocator) !Self {
            var wal = w.Wal{};
            try wal.open();

            var node_index_storage = nis.NodeIndexStorage{};
            try node_index_storage.open();

            var storage = Self{
                .path = path,
                .max_memtable_size = max_memtable_size,
                .memtable_level_probability = 0.125,
                .allocator = allocator,
                .wal = wal,
                .node_index_storage = node_index_storage,
                .memtables = null,
            };

            if (try storage.wal.is_empty() == true) {
                // TODO: create memtable from wal
            }

            return storage;
        }

        pub fn destroy(self: *Self) void {
            self.wal.close();
            self.node_index_storage.close();
            if (self.memtables) |ts| {
                for (ts.items) |t| {
                    t.destroy();
                    self.allocator.destroy(t);
                }
                ts.deinit();
                self.memtables = null;
            }
        }

        pub fn write(self: *Self, comptime T: type, value: T) !void {
            const record = try self.allocator.create(NodeRecord);
            record.* = .{
                .value_type = try value_type_from_type(T),
                .value_size = @sizeOf(T),
                .value = &mt.key_from_int_data(T, value),
            };
            defer self.allocator.destroy(record);

            try self.wal.write(record);
            _ = try self.node_index_storage.allocate_next_id(); // TODO: use the node_id value

            if (self.memtables == null) {
                self.memtables = try ArrayList(*mt.Memtable(N)).initCapacity(self.allocator, 2);
                try self.add_memtable();
            } else if (self.memtables.?.getLast().size >= self.max_memtable_size) {
                const filled_memtable = self.memtables.?.pop();
                var file_name_buf: [24]u8 = undefined;
                const file_name = try std.fmt.bufPrint(
                    &file_name_buf,
                    "0.{d}.sstable",
                    .{try self.next_sstable_index(0)},
                );
                const sstable = try st.SSTable.create(
                    N,
                    filled_memtable,
                    file_name,
                );
                sstable.close();
                filled_memtable.destroy();

                try self.add_memtable();
            }

            const current_memtable = self.memtables.?.getLast();
            try current_memtable.*.add(
                record.value,
                mt.MemtableValue{
                    .first_relationship_pointer = 0,
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
            memtable.* = mt.Memtable(N).init(
                self.allocator,
                rng.random(),
                self.memtable_level_probability,
            );
            try self.memtables.?.append(memtable);
        }

        fn next_sstable_index(self: Self, level: u8) !i32 {
            var dir = try std.fs.cwd().openDir(self.path, .{
                .access_sub_paths = false,
                .iterate = true,
                .no_follow = true,
            });
            defer dir.close();

            var buf: [4]u8 = undefined;
            const level_str = try std.fmt.bufPrint(&buf, "{}", .{level});

            var last_index: i32 = -1;
            var it = dir.iterate();
            while (try it.next()) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".sstable") and std.mem.startsWith(u8, entry.name, level_str)) {
                    const file_name = entry.name;
                    const first_dot = std.mem.indexOfScalar(u8, file_name, '.').?; // TODO: handle return value correctly
                    const second_dot = std.mem.indexOfScalar(u8, file_name[first_dot + 1 ..], '.').? + first_dot + 1;
                    const second_number_str = file_name[first_dot + 1 .. second_dot];
                    last_index = try std.fmt.parseInt(i32, second_number_str, 10);
                }
            }

            return last_index + 1;
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
    };
}

// Tests
const testing = std.testing;

test "Add value" {
    var test_storage = try Storage(8).init("./", 8, testing.allocator);
    try test_storage.write(u64, 1);
    test_storage.destroy();
}
