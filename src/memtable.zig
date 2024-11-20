/// Skiplist implementation of a memtable for graph nodes
const std = @import("std");
const data_types = @import("./data_types.zig");
const w = @import("./wal.zig");
const utils = @import("./utils.zig");

const ValueType = data_types.ValueType;

pub const MemtableKey = []const u8;

pub const MemtableValue = struct {
    node_id: u64,
    value_type: data_types.ValueType,
    value_size: u16,
};

pub const Memtable = struct {
    allocator: std.mem.Allocator,
    rng: std.Random,
    max_level: u8,
    level_probability: f32,
    level: u8 = 1,
    wal_name: []const u8,
    wal: w.Wal,
    compare_fn: *const fn ([]const u8, []const u8) isize = utils.compare_bitwise,
    head: ?*MemtableNode = null,
    size: u16 = 0,

    const MemtableNode = struct {
        key: ?[]u8,
        value: ?MemtableValue,
        tower: []?*MemtableNode,
    };

    const MemtableIterator = struct {
        current: ?*MemtableNode,

        pub inline fn next(self: *MemtableIterator) ?*MemtableNode {
            if (self.current) |c| {
                self.current = c.tower[0];
                return self.current;
            } else {
                return null;
            }
        }
    };

    pub fn init(allocator: std.mem.Allocator, random: std.Random, max_level: u8, level_probability: f32) !Memtable {
        const wal_name = try allocator.alloc(u8, 8);
        const wal_id = utils.generate_id(random);

        var wal = w.Wal{ .path = try std.fmt.bufPrint(
            wal_name,
            "{x:0>2}{x:0>2}.wal",
            .{ wal_id[0], wal_id[1] },
        ) };
        try wal.open();

        return Memtable{
            .allocator = allocator,
            .rng = random,
            .max_level = max_level,
            .level_probability = level_probability,
            .wal_name = wal_name,
            .wal = wal,
        };
    }

    pub fn from_wal(wal: w.Wal, allocator: std.mem.Allocator, random: std.Random, max_level: u8, level_probability: f32) !Memtable {
        var memtable = Memtable{
            .allocator = allocator,
            .rng = random,
            .max_level = max_level,
            .level_probability = level_probability,
            .wal_name = wal.path,
            .wal = wal,
        };
        try memtable.wal.open();

        const record_buffer = try allocator.alloc(u8, 11);
        defer allocator.free(record_buffer);

        while (try memtable.wal.read_record(record_buffer) == 11) {
            const node_id: u64 = std.mem.readInt(u64, record_buffer[0..8], std.builtin.Endian.big);
            const value_type: u8 = std.mem.readInt(u8, record_buffer[8..9], std.builtin.Endian.big);
            const value_size: u16 = std.mem.readInt(u16, record_buffer[9..11], std.builtin.Endian.big);
            const value_buffer = try allocator.alloc(u8, value_size);
            _ = try memtable.wal.file.?.readAll(value_buffer);
            try memtable.add(value_buffer, MemtableValue{
                .node_id = node_id,
                .value_type = @enumFromInt(value_type),
                .value_size = value_size,
            });
            allocator.free(value_buffer);
        }

        return memtable;
    }

    pub fn add(self: *Memtable, key: MemtableKey, value: MemtableValue) !void {
        if (self.head == null) try self.create_head();

        var path: []?*MemtableNode = try self.allocator.alloc(?*MemtableNode, self.max_level);
        defer self.allocator.free(path);
        for (0..self.max_level) |i| {
            path[i] = null;
        }

        if (self.search(key, path[0..])) |_| {
            return;
        } else {
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
    }

    pub fn find(self: *const Memtable, key: MemtableKey) ?MemtableValue {
        if (self.head == null) return null;

        const result = self.search(key, null) orelse return null;
        return result.*.value;
    }

    pub fn destroy(self: *Memtable) void {
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

    pub fn iterator(self: *const Memtable) MemtableIterator {
        return MemtableIterator{ .current = self.head };
    }

    fn search(self: *const Memtable, key: MemtableKey, path: ?[]?*MemtableNode) ?*MemtableNode {
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

    inline fn pick_level(self: *Memtable) u8 {
        var level: u8 = 1;
        while (level < self.max_level and self.rng.float(f32) > (1 - self.level_probability)) {
            level += 1;
        }
        return level;
    }

    fn create_head(self: *Memtable) !void {
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

// Tests
const testing = std.testing;

inline fn test_value() MemtableValue {
    return MemtableValue{
        .node_id = 0,
        .value_size = 4,
        .value_type = ValueType.int,
    };
}

fn size(table: Memtable) i32 {
    var result: i32 = -1;
    var node_cursor = table.head;
    while (node_cursor != null) {
        result += 1;
        node_cursor = node_cursor.?.tower[0];
    }
    return result;
}

fn visualize_memtable(table: Memtable) !void {
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

test "compare_bitwise" {
    // Case 1: empty arrays
    const a1 = [_]u8{};
    const a2 = [_]u8{};
    try testing.expect(utils.compare_bitwise(&a1, &a2) == 0);

    // Case 2: arrays of zero of different size
    const a3 = [1]u8{0};
    const a4 = [2]u8{ 0, 0 };
    try testing.expect(utils.compare_bitwise(&a3, &a4) < 0);
    try testing.expect(utils.compare_bitwise(&a4, &a3) > 0);

    // Case 3: empty array vs array of zero
    try testing.expect(utils.compare_bitwise(&a1, &a3) < 0);

    // Case 4: arrays of zero of the same size
    const a5 = [2]u8{ 0, 0 };
    try testing.expect(utils.compare_bitwise(&a4, &a5) == 0);

    // Case 5: arrays of the same size
    const a6 = [4]u8{ 0, 0, 0, 0 };
    const a7 = [4]u8{ 0, 0, 0, 1 };
    const a8 = [4]u8{ 0, 0, 0, 2 };
    try testing.expect(utils.compare_bitwise(&a6, &a7) < 0);
    try testing.expect(utils.compare_bitwise(&a7, &a8) < 0);

    // Case 6: arrays of different size
    const a9 = [2]u8{ 0, 1 };
    try testing.expect(utils.compare_bitwise(&a9, &a6) > 0);
    try testing.expect(utils.compare_bitwise(&a4, &a7) < 0);
}

test "Memtable#add and find" {
    var rng = std.rand.DefaultPrng.init(blk: {
        var seed: u64 = undefined;
        try std.posix.getrandom(std.mem.asBytes(&seed));
        break :blk seed;
    });
    var test_memtable = try Memtable.init(testing.allocator, rng.random(), 8, 0.125);
    defer test_memtable.destroy();

    // Case: search in an empty memtable
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, 1)) == null);

    // Case: a key is added succesfully to an empty memtable
    const test_vertex_data = test_value();
    const test_vertex0 = utils.key_from_int_data(u8, 0);
    try test_memtable.add(&test_vertex0, test_vertex_data);
    try testing.expect(test_memtable.head != null);
    try testing.expect(std.mem.eql(u8, test_memtable.head.?.tower[0].?.key.?, &test_vertex0));
    try testing.expect(size(test_memtable) == 1);
    try testing.expect(test_memtable.size == 1);

    // Case: same key won't be added twice, no duplicates are allowed
    const test_vertex1 = utils.key_from_int_data(u8, 0);
    try test_memtable.add(&test_vertex1, test_vertex_data);
    try testing.expect(size(test_memtable) == 1);
    try testing.expect(test_memtable.size == 1);

    // Case: adding more keys
    var table_size: u8 = 1;
    for (0..16) |_| {
        try test_memtable.add(&utils.key_from_int_data(u8, table_size), test_vertex_data);
        table_size += 1;
        try testing.expect(size(test_memtable) == table_size);
        try testing.expect(test_memtable.size == table_size);
    }

    // Case: find
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, 0)) != null);
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, table_size / 2)) != null);
    try testing.expect(test_memtable.find(&utils.key_from_int_data(u8, table_size + 1)) == null);

    try test_memtable.wal.delete_file();
}
