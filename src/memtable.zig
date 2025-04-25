/// Skiplist implementation of a memtable for graph nodes
const std = @import("std");

const data_types = @import("./data_types.zig");
const global_context = @import("./global_context.zig");
const w = @import("./wal.zig");
const utils = @import("./utils.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const StorageRecord = data_types.StorageRecord;

pub const Memtable = struct {
    allocator: std.mem.Allocator,
    rng: std.Random,
    max_size: u16,
    max_level: u8,
    level_probability: f32,
    level: u8 = 1,
    wal: w.Wal,
    compare_fn: *const fn (data_types.BinaryData, data_types.BinaryData) isize = utils.compare_bitwise,
    head: ?*MemtableNode = null,
    size: u16 = 0,
    lock: std.Thread.Mutex = .{},

    pub const IsFull = bool;

    const MemtableNode = struct {
        key: ?[]u8,
        value: ?[]u8,
        tower: []?*MemtableNode,
    };

    const MemtableIterator = struct {
        allocator: std.mem.Allocator,
        current: ?*MemtableNode,

        pub inline fn next(self: *MemtableIterator) ?StorageRecord {
            if (self.current) |c| {
                self.current = c.tower[0];
                if (c.key) |key| {
                    return StorageRecord{
                        .allocator = self.allocator,
                        .key_size = @as(u16, @intCast(key.len)),
                        .key = key,
                        .value_size = @as(u16, @intCast(c.value.?.len)),
                        .value = c.value.?,
                    };
                }
            } else {
                return null;
            }

            return null;
        }
    };

    pub inline fn init(allocator: std.mem.Allocator, random: std.Random, max_size: u16, max_level: u8, level_probability: f32, wal_path: []const u8) !Memtable {
        const wal_name = try allocator.alloc(u8, wal_path.len + 9);
        const wal_id = utils.generate_id(random);

        const wal = try w.Wal.open(
            allocator,
            try std.fmt.bufPrint(
                wal_name,
                "{s}/{x:0>2}{x:0>2}.wal",
                .{ wal_path, wal_id[0], wal_id[1] },
            ),
        );

        return Memtable{
            .allocator = allocator,
            .rng = random,
            .max_size = max_size,
            .max_level = max_level,
            .level_probability = level_probability,
            .wal = wal,
        };
    }

    pub fn create(allocator: std.mem.Allocator, path: []const u8) !*Memtable {
        const config = global_context.get_configurator().?;
        const memtable = try allocator.create(Memtable);
        memtable.* = try Memtable.init(
            allocator,
            std.crypto.random,
            config.memtable_max_size(),
            config.memtable_max_level(),
            config.memtable_level_probability(),
            path,
        );
        return memtable;
    }

    pub fn from_wal(wal: w.Wal, memtable: *Memtable) !IsFull {
        while (wal.read_record(memtable.allocator)) |record| {
            defer record.destroy();
            if (!memtable.is_full()) {
                try memtable.wal.write(&record);
                try memtable.add(record.key, record.value);
            } else {
                return true;
            }
        } else |_| {}

        return false;
    }

    pub fn add(self: *Memtable, key: data_types.BinaryData, value: data_types.BinaryData) !void {
        if (!utils.try_lock_for(&self.lock, 200)) return ApplicationError.ExecutionTimeout; // TODO: try to re-queue the task
        defer self.lock.unlock();

        if (self.head == null) try self.create_head();

        var path: []?*MemtableNode = try self.allocator.alloc(?*MemtableNode, self.max_level);
        defer self.allocator.free(path);
        for (0..self.max_level) |i| {
            path[i] = null;
        }

        if (self.search(key, path[0..])) |node| {
            self.allocator.free(node.*.value.?);
            node.*.value = try self.allocator.alloc(u8, value.len);
            @memcpy(node.*.value.?, value);
        } else {
            const new_node_level = self.pick_level();
            self.level = @max(self.level, new_node_level);
            const new_node = try self.allocator.create(MemtableNode);
            new_node.* = .{
                .key = try self.allocator.alloc(u8, key.len),
                .value = try self.allocator.alloc(u8, value.len),
                .tower = try self.allocator.alloc(?*MemtableNode, self.max_level),
            };
            for (0..self.max_level) |i| {
                new_node.*.tower[i] = null;
            }
            @memcpy(new_node.*.key.?, key);
            @memcpy(new_node.*.value.?, value);

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

    pub inline fn find(self: *Memtable, key: data_types.BinaryData, lock: bool) !?data_types.BinaryData {
        if (lock) {
            if (!utils.try_lock_for(&self.lock, 200)) return ApplicationError.ExecutionTimeout;
        }
        defer {
            if (lock) self.lock.unlock();
        }

        if (self.head == null) return null;

        const result = self.search(key, null) orelse return null;
        return result.*.value;
    }

    pub fn destroy(self: *const Memtable) void {
        var current = self.head;
        var next = current;
        while (current != null) {
            next = current.?.tower[0];
            if (current.?.key) |key| {
                self.allocator.free(key);
                self.allocator.free(current.?.value.?);
            }
            self.allocator.free(current.?.tower);
            self.allocator.destroy(current.?);
            current = next;
        }
        self.wal.close_and_free();
    }

    pub inline fn is_full(self: *const Memtable) bool {
        return self.size >= self.max_size;
    }

    pub inline fn iterator(self: *const Memtable) MemtableIterator {
        return MemtableIterator{
            .allocator = self.allocator,
            .current = if (self.head != null) self.head.?.tower[0] else null,
        };
    }

    fn search(self: *const Memtable, key: data_types.BinaryData, path: ?[]?*MemtableNode) ?*MemtableNode {
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

    inline fn pick_level(self: *const Memtable) u8 {
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

test "Memtable#add and find" {
    var test_memtable = try Memtable.init(testing.allocator, std.crypto.random, 64, 8, 0.125, "./");
    errdefer test_memtable.destroy();

    // Case: search in an empty memtable
    try testing.expect(try test_memtable.find(&utils.int_to_bytes(u8, 1), false) == null);

    // Case: a key is added succesfully to an empty memtable
    const test_value = utils.int_to_bytes(u8, 0);
    const test_key0 = utils.int_to_bytes(u8, 0);
    try test_memtable.add(&test_key0, &test_value);
    try testing.expect(test_memtable.head != null);
    try testing.expect(std.mem.eql(u8, test_memtable.head.?.tower[0].?.key.?, &test_key0));
    try testing.expect(test_memtable.size == 1);

    // Case: adding the same key twice
    const test_key1 = utils.int_to_bytes(u8, 0);
    try test_memtable.add(&test_key1, &test_value);
    try testing.expect(test_memtable.size == 1);

    // Case: adding more keys
    var table_size: u8 = 1;
    for (0..16) |_| {
        try test_memtable.add(&utils.int_to_bytes(u8, table_size), &test_value);
        table_size += 1;
        try testing.expect(test_memtable.size == table_size);
    }

    // Case: find
    try testing.expect(try test_memtable.find(&utils.int_to_bytes(u8, 0), false) != null);
    try testing.expect(try test_memtable.find(&utils.int_to_bytes(u8, table_size / 2), false) != null);
    try testing.expect(try test_memtable.find(&utils.int_to_bytes(u8, table_size + 1), false) == null);

    try test_memtable.wal.delete_file();
    test_memtable.destroy();
}
