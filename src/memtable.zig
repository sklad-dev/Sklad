/// Skiplist implementation of a memtable for graph nodes
const std = @import("std");
const data_types = @import("./data_types.zig");
const w = @import("./wal.zig");
const utils = @import("./utils.zig");

const StorageRecord = data_types.StorageRecord;

pub const MemtableKey = []const u8;

pub fn Memtable(comptime V: type) type {
    return struct {
        allocator: std.mem.Allocator,
        rng: std.Random,
        max_level: u8,
        level_probability: f32,
        level: u8 = 1,
        wal_name: []const u8,
        wal: w.Wal(V),
        compare_fn: *const fn ([]const u8, []const u8) isize = utils.compare_bitwise,
        head: ?*MemtableNode = null,
        size: u16 = 0,

        const Self = @This();

        const MemtableNode = struct {
            key: ?[]u8,
            value: ?V,
            tower: []?*MemtableNode,
        };

        const MemtableIterator = struct {
            current: ?*MemtableNode,

            pub inline fn next(self: *MemtableIterator) ?StorageRecord(V) {
                if (self.current) |c| {
                    self.current = c.tower[0];
                    if (c.key) |key| {
                        return StorageRecord(V){
                            .key_size = @as(u16, @intCast(key.len)),
                            .key = key,
                            .value = c.value.?,
                        };
                    }
                } else {
                    return null;
                }

                return null;
            }
        };

        pub inline fn init(allocator: std.mem.Allocator, random: std.Random, max_level: u8, level_probability: f32) !Self {
            const wal_name = try allocator.alloc(u8, 8);
            const wal_id = utils.generate_id(random);

            var wal = w.Wal(V){ .path = try std.fmt.bufPrint(
                wal_name,
                "{x:0>2}{x:0>2}.wal",
                .{ wal_id[0], wal_id[1] },
            ) };
            try wal.open();

            return Self{
                .allocator = allocator,
                .rng = random,
                .max_level = max_level,
                .level_probability = level_probability,
                .wal_name = wal_name,
                .wal = wal,
            };
        }

        pub fn from_wal(wal: w.Wal(V), allocator: std.mem.Allocator, random: std.Random, max_level: u8, level_probability: f32) !Self {
            var memtable = Self{
                .allocator = allocator,
                .rng = random,
                .max_level = max_level,
                .level_probability = level_probability,
                .wal_name = wal.path,
                .wal = wal,
            };
            try memtable.wal.open();

            while (memtable.wal.read_record(allocator)) |record| {
                try memtable.add(record.key, record.value);
                allocator.free(record.key);
            } else |_| {}

            return memtable;
        }

        pub fn add(self: *Self, key: MemtableKey, value: V) !void {
            if (self.head == null) try self.create_head();
            var path: []?*MemtableNode = try self.allocator.alloc(?*MemtableNode, self.max_level);
            defer self.allocator.free(path);
            for (0..self.max_level) |i| {
                path[i] = null;
            }

            _ = self.search(key, path[0..]);
            const new_node_level = self.pick_level();
            self.level = @max(self.level, new_node_level);
            const new_node = try self.allocator.create(MemtableNode);
            new_node.* = .{
                .key = try self.allocator.alloc(u8, key.len),
                .value = value,
                .tower = try self.allocator.alloc(?*MemtableNode, self.max_level),
            };
            for (0..self.max_level) |i| {
                new_node.*.tower[i] = null;
            }
            @memcpy(new_node.*.key.?, key);
            for (path, 0..) |node, i| {
                if (node) |n| {
                    new_node.tower[i] = n.tower[i];
                    n.tower[i] = new_node;
                } else if (i < self.level) {
                    self.head.?.tower[i] = new_node;
                }
            }
            self.size += 1;
        }

        pub inline fn find(self: *const Self, key: MemtableKey) ?V {
            if (self.head == null) return null;

            const result = self.search(key, null) orelse return null;
            return result.*.value;
        }

        pub fn destroy(self: *Self) void {
            var current = self.head;
            var next = current;
            while (current != null) {
                next = current.?.tower[0];
                if (current.?.key) |key| {
                    self.allocator.free(key);
                }
                self.allocator.free(current.?.tower);
                self.allocator.destroy(current.?);
                current = next;
            }
            self.wal.close();
            self.allocator.free(self.wal_name);
        }

        pub inline fn iterator(self: *const Self) MemtableIterator {
            return MemtableIterator{ .current = if (self.head != null) self.head.?.tower[0] else null };
        }

        fn search(self: *const Self, key: MemtableKey, path: ?[]?*MemtableNode) ?*MemtableNode {
            var cursor = self.head.?;
            var l = self.level;
            while (l > 0) {
                l -= 1;
                while (cursor.tower[l] != null and self.compare_fn(cursor.tower[l].?.*.key.?, key) < 0) {
                    cursor = cursor.tower[l].?;
                }
                if (path) |p| {
                    p[l] = cursor;
                }
                if (cursor.tower[l] != null and self.compare_fn(cursor.tower[l].?.*.key.?, key) == 0) {
                    return cursor.tower[l];
                }
            }
            return null;
        }

        inline fn pick_level(self: *Self) u8 {
            var level: u8 = 1;
            while (level < self.max_level and self.rng.float(f32) > (1 - self.level_probability)) {
                level += 1;
            }
            return level;
        }

        fn create_head(self: *Self) !void {
            if (self.head != null) return;

            const head = try self.allocator.create(MemtableNode);
            head.* = .{
                .key = null,
                .value = null,
                .tower = try self.allocator.alloc(?*MemtableNode, self.max_level),
            };
            for (0..self.max_level) |i| {
                head.*.tower[i] = null;
            }
            self.head = head;
        }
    };
}

// Tests
const testing = std.testing;

fn visualize_memtable(comptime V: type, table: Memtable(V)) !void {
    const allocator = std.testing.allocator;
    var representation = std.ArrayList(u8).init(allocator);
    defer representation.deinit();

    var l = table.level;
    std.debug.print("Memtable, heights = {d}, size = {d}\n", .{ l, table.size });
    while (l > 0) {
        l -= 1;
        var empty = true;
        var node_cursor = table.head;
        while (node_cursor != null) {
            empty = false;
            if (node_cursor.?.key) |k| {
                try representation.writer().print("[{d}] -> ", .{k.items});
            } else {
                try representation.writer().print("[null] -> ", .{});
            }
            node_cursor = node_cursor.?.tower[l];
        }
        if (!empty) {
            try representation.writer().print("\n", .{});
        }
    }
    std.debug.print("{s}\n", .{representation.items});
}

test "Memtable#add and find" {
    var test_memtable = try Memtable(u8).init(testing.allocator, std.crypto.random, 8, 0.125);
    errdefer test_memtable.destroy();

    // Case: search in an empty memtable
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, 1)) == null);

    // Case: a key is added succesfully to an empty memtable
    const test_vertex_data: u8 = 0;
    const test_vertex0 = utils.key_from_int_data(u8, 0);
    try test_memtable.add(&test_vertex0, test_vertex_data);
    try testing.expect(test_memtable.head != null);
    try testing.expect(std.mem.eql(u8, test_memtable.head.?.tower[0].?.key.?, &test_vertex0));
    try testing.expect(test_memtable.size == 1);

    // Case: adding the same key twice
    const test_vertex1 = utils.key_from_int_data(u8, 0);
    try test_memtable.add(&test_vertex1, test_vertex_data);
    try testing.expect(test_memtable.size == 2);

    // Case: adding more keys
    var table_size: u8 = 2;
    for (0..16) |_| {
        try test_memtable.add(&utils.key_from_int_data(u8, table_size), test_vertex_data);
        table_size += 1;
        try testing.expect(test_memtable.size == table_size);
    }

    // Case: find
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, 0)) != null);
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, table_size / 2)) != null);
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, table_size + 1)) == null);

    try test_memtable.wal.delete_file();
    test_memtable.destroy();
}
