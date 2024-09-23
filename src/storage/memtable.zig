/// Skiplist implementation of a memtable for graph nodes
const std = @import("std");
const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;

pub const MemtableKey = []const u8;

pub const MemtableValue = struct {
    first_relationship_pointer: usize,
    value_type: data_types.ValueType,
    value_size: u32,
};

pub fn keyFromIntData(comptime T: type, key_value: T) [@sizeOf(T)]u8 {
    var buffer: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buffer, key_value, std.builtin.Endian.big);
    return buffer;
}

pub fn Memtable(comptime N: u8) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        rng: std.Random,
        level_probability: f32,
        level: u8 = 1,
        compare_fn: *const fn ([]const u8, []const u8) isize = compare_bitwise,
        head: ?*MemtableNode = null,

        const MemtableNode = struct {
            key: ?std.ArrayList(u8),
            value: ?MemtableValue,
            tower: [N]?*MemtableNode,
        };

        pub fn init(allocator: std.mem.Allocator, random: std.Random, level_probability: f32) Self {
            return Self{
                .allocator = allocator,
                .rng = random,
                .level_probability = level_probability,
            };
        }

        pub fn add(self: *Self, key: MemtableKey, value: MemtableValue) !void {
            if (self.head == null) try self.create_head();

            var path: [N]?*MemtableNode = [_]?*MemtableNode{null} ** N;
            if (self.search(key, path[0..])) |_| {
                return;
            } else {
                const new_node_level = self.pick_level();
                self.level = @max(self.level, new_node_level);
                const new_node = try self.allocator.create(MemtableNode);
                new_node.* = .{
                    .key = try std.ArrayList(u8).initCapacity(self.allocator, key.len),
                    .value = value,
                    .tower = [_]?*MemtableNode{null} ** N,
                };
                try new_node.*.key.?.appendSlice(key);
                for (path, 0..) |node, i| {
                    if (node) |n| {
                        new_node.tower[i] = n.tower[i];
                        n.tower[i] = new_node;
                    } else if (i < self.level) {
                        self.head.?.tower[i] = new_node;
                    }
                }
            }
        }

        pub fn find(self: Self, key: MemtableKey) ?MemtableValue {
            if (self.head == null) return null;

            const result = self.search(key, null) orelse return null;
            return result.*.value;
        }

        pub fn destroy(self: Self) void {
            var current = self.head;
            var next = current;
            while (current != null) {
                next = current.?.tower[0];
                if (current.?.key) |key| {
                    key.deinit();
                }
                self.allocator.destroy(current.?);
                current = next;
            }
        }

        fn search(self: Self, key: MemtableKey, path: ?[]?*MemtableNode) ?*MemtableNode {
            var cursor = self.head.?;
            var l = self.level;
            while (l > 0) {
                l -= 1;
                while (cursor.tower[l] != null and self.compare_fn(cursor.tower[l].?.*.key.?.items, key) < 0) {
                    cursor = cursor.tower[l].?;
                }
                if (path) |p| {
                    p[l] = cursor;
                }
                if (cursor.tower[l] != null and self.compare_fn(cursor.tower[l].?.*.key.?.items, key) == 0) {
                    return cursor.tower[l];
                }
            }
            return null;
        }

        fn pick_level(self: *Self) u8 {
            var level: u8 = 1;
            while (level < N and self.rng.float(f32) > (1 - self.level_probability)) {
                level += 1;
            }
            return level;
        }

        fn create_head(self: *Self) !void {
            if (self.head != null) {
                return;
            }
            const head = try self.allocator.create(MemtableNode);
            head.* = .{
                .key = null,
                .value = null,
                .tower = [_]?*MemtableNode{null} ** N,
            };
            self.head = head;
        }
    };
}

fn compare_bitwise(v1: []const u8, v2: []const u8) isize {
    if (v1.len == v2.len and std.mem.eql(u8, v1, v2)) return 0;

    const min_length = @min(v1.len, v2.len);
    for (0..min_length) |i| {
        if (v1[i] != v2[i]) {
            return @as(isize, @intCast(v1[i])) - @as(isize, @intCast(v2[i]));
        }
    }

    return @as(isize, @intCast(v1.len)) - @as(isize, @intCast(v2.len));
}

// Tests
const testing = std.testing;

fn test_value() MemtableValue {
    return MemtableValue{
        .first_relationship_pointer = 0,
        .value_size = 4,
        .value_type = ValueType.int,
    };
}

fn size(comptime N: usize, table: Memtable(N)) i32 {
    var result: i32 = -1;
    var node_cursor = table.head;
    while (node_cursor != null) {
        result += 1;
        node_cursor = node_cursor.?.tower[0];
    }
    return result;
}

fn visualize_memtable(comptime N: usize, table: Memtable(N)) !void {
    const allocator = std.testing.allocator;
    var representation = std.ArrayList(u8).init(allocator);
    defer representation.deinit();

    var l = table.level;
    std.debug.print("Memtable, heights = {d}\n", .{l});
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

test "compare_bitwise" {
    // Case 1: empty arrays
    const a1 = [_]u8{};
    const a2 = [_]u8{};
    try testing.expect(compare_bitwise(&a1, &a2) == 0);

    // Case 2: arrays of zero of different size
    const a3 = [1]u8{0};
    const a4 = [2]u8{ 0, 0 };
    try testing.expect(compare_bitwise(&a3, &a4) < 0);
    try testing.expect(compare_bitwise(&a4, &a3) > 0);

    // Case 3: empty array vs array of zero
    try testing.expect(compare_bitwise(&a1, &a3) < 0);

    // Case 4: arrays of zero of the same size
    const a5 = [2]u8{ 0, 0 };
    try testing.expect(compare_bitwise(&a4, &a5) == 0);

    // Case 5: arrays of the same size
    const a6 = [4]u8{ 0, 0, 0, 0 };
    const a7 = [4]u8{ 0, 0, 0, 1 };
    const a8 = [4]u8{ 0, 0, 0, 2 };
    try testing.expect(compare_bitwise(&a6, &a7) < 0);
    try testing.expect(compare_bitwise(&a7, &a8) < 0);

    // Case 6: arrays of different size
    const a9 = [2]u8{ 0, 1 };
    try testing.expect(compare_bitwise(&a9, &a6) > 0);
    try testing.expect(compare_bitwise(&a4, &a7) < 0);
}

test "Memtable#add and find" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = Memtable(8).init(testing.allocator, rng.random(), 0.125);
    defer test_memtable.destroy();

    // Case: search in an empty memtable
    try testing.expect(test_memtable.find(&keyFromIntData(u8, 1)) == null);

    // Case: a key is added succesfully to an empty memtable
    const test_vertex_data = test_value();
    const test_vertex0 = keyFromIntData(u8, 0);
    try test_memtable.add(&test_vertex0, test_vertex_data);
    try testing.expect(test_memtable.head != null);
    try testing.expect(std.mem.eql(u8, test_memtable.head.?.tower[0].?.key.?.items, &test_vertex0));
    try testing.expect(size(8, test_memtable) == 1);

    // Case: same key won't be added twice, no duplicates are allowed
    const test_vertex1 = keyFromIntData(u8, 0);
    try test_memtable.add(&test_vertex1, test_vertex_data);
    try testing.expect(size(8, test_memtable) == 1);

    // Case: adding more keys
    var table_size: u8 = 1;
    for (0..16) |_| {
        try test_memtable.add(&keyFromIntData(u8, table_size), test_vertex_data);
        table_size += 1;
        try testing.expect(size(8, test_memtable) == table_size);
    }

    // Case: find
    try testing.expect(test_memtable.find(&keyFromIntData(u8, 0)) != null);
    try testing.expect(test_memtable.find(&keyFromIntData(u8, table_size / 2)) != null);
    try testing.expect(test_memtable.find(&keyFromIntData(u8, table_size + 1)) == null);
}
