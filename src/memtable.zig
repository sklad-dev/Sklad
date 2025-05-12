const std = @import("std");

const data_types = @import("./data_types.zig");
const utils = @import("./utils.zig");

const getConfigurator = @import("./global_context.zig").getConfigurator;
const Wal = @import("./wal.zig").Wal;
const BinaryData = data_types.BinaryData;
const StorageRecord = data_types.StorageRecord;

pub const Arena = struct {
    allocator: std.mem.Allocator,
    arena: []u8,
    current_offset: u64 = 0,

    const ALIGNMENT: u64 = @alignOf(usize);

    pub const StorageError = error{
        ArenaIsFull,
    };

    pub fn init(allocator: std.mem.Allocator, arena_size: u64) !Arena {
        return .{
            .allocator = allocator,
            .arena = try allocator.alloc(u8, arena_size),
        };
    }

    pub fn reserve(self: *Arena, data_size: u64) !u64 {
        var current_offset: u64 = undefined;
        var new_offset: u64 = undefined;

        while (true) {
            current_offset = @atomicLoad(u64, &self.current_offset, .seq_cst);
            new_offset = ((current_offset + data_size) + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
            if (new_offset > self.arena.len) {
                return StorageError.ArenaIsFull;
            }

            if (@cmpxchgWeak(u64, &self.current_offset, current_offset, new_offset, .seq_cst, .seq_cst) != null) {
                continue;
            }
            break;
        }
        return current_offset;
    }

    pub inline fn currentOffset(self: *const Arena) u64 {
        return @atomicLoad(u64, &self.current_offset, .seq_cst);
    }

    pub fn deinit(self: *Arena) void {
        self.allocator.free(self.arena);
    }
};

pub const Memtable = struct {
    allocator: std.mem.Allocator,
    arena: Arena,
    max_size: u64,
    max_level: u8,
    rng: std.Random,
    wal: Wal,
    size: u32 = 0,
    compare_fn: *const fn (BinaryData, BinaryData) isize = utils.compareBitwise,

    const Node = struct {
        key: ?[]u8,
        value: ?[]u8,
        tower: []u64,
    };

    const MemtableIterator = struct {
        arena: *const Arena,
        current: ?*Node,

        pub inline fn next(self: *MemtableIterator) ?StorageRecord {
            if (self.current) |c| {
                self.current = @alignCast(@ptrCast(&self.arena.arena[c.tower[0]]));
                if (c.key) |key| {
                    return StorageRecord{
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

    pub inline fn init(allocator: std.mem.Allocator, random: std.Random, max_size: u64, max_level: u8, wal_path: []const u8) !Memtable {
        const wal_name = try allocator.alloc(u8, wal_path.len + 9);
        const wal_id = utils.generateId(random);

        const wal = try Wal.open(
            allocator,
            try std.fmt.bufPrint(
                wal_name,
                "{s}/{x:0>2}{x:0>2}.wal",
                .{ wal_path, wal_id[0], wal_id[1] },
            ),
        );

        var arena = try Arena.init(allocator, max_size);

        const node_and_tower_size: u64 = @sizeOf(Node) + (max_level + 1) * @sizeOf(u64);
        _ = try arena.reserve(node_and_tower_size);
        const head_tower_ptr: [*]u64 = @alignCast(@ptrCast(arena.arena[@sizeOf(Node)..node_and_tower_size].ptr));
        var head: *Node = @alignCast(@ptrCast(&arena.arena[0]));
        head.* = .{
            .key = null,
            .value = null,
            .tower = head_tower_ptr[0 .. max_level + 1],
        };

        const tail_offset_start = try arena.reserve(node_and_tower_size);
        const tail_offset_end: u64 = tail_offset_start + node_and_tower_size;
        const tail_tower_ptr: [*]u64 = @alignCast(@ptrCast(arena.arena[tail_offset_start + @sizeOf(Node) .. tail_offset_end].ptr));
        var tail: *Node = @alignCast(@ptrCast(&arena.arena[tail_offset_start]));
        tail.* = .{
            .key = null,
            .value = null,
            .tower = tail_tower_ptr[0 .. max_level + 1],
        };

        for (0..max_level + 1) |i| {
            tail.tower[i] = 0;
            head.tower[i] = tail_offset_start;
        }

        return Memtable{
            .allocator = allocator,
            .arena = arena,
            .max_size = max_size,
            .max_level = max_level,
            .rng = random,
            .wal = wal,
        };
    }

    pub fn create(allocator: std.mem.Allocator, path: []const u8) !*Memtable {
        const config = getConfigurator().?;
        const memtable = try allocator.create(Memtable);
        memtable.* = try Memtable.init(
            allocator,
            std.crypto.random,
            config.memtableMaxSize(),
            config.memtableMaxLevel(),
            path,
        );
        return memtable;
    }

    pub fn destroy(self: *Memtable) void {
        self.arena.deinit();
        self.wal.closeAndFree();
    }

    pub fn fromWal(wal: Wal, memtable: *Memtable) !bool {
        while (wal.readRecord(memtable.allocator)) |record| {
            defer record.destroy(memtable.allocator);
            if (memtable.canAdd(record.key_size + record.value_size)) {
                try memtable.wal.write(&record);
                try memtable.add(record.key, record.value);
            } else {
                return true;
            }
        } else |_| {}

        return false;
    }

    pub fn add(self: *Memtable, key: BinaryData, value: BinaryData) !void {
        const new_node_height = self.pickLevel();

        var predecessors: []u64 = try self.allocator.alloc(u64, self.max_level + 1);
        defer self.allocator.free(predecessors);

        var successors: []u64 = try self.allocator.alloc(u64, self.max_level + 1);
        defer self.allocator.free(successors);

        for (0..self.max_level + 1) |i| {
            predecessors[i] = 0;
            successors[i] = 0;
        }

        var created: bool = false;
        var new_node: ?*Node = null;
        var new_node_offset: u64 = 0;
        while (true) {
            const found_index = self.search(key, predecessors, successors);
            if (found_index != 0) {
                const node_to_update: *Node = @alignCast(@ptrCast(&self.arena.arena[found_index]));
                const value_offset = try self.arena.reserve(value.len);
                const value_ptr: [*]u8 = @alignCast(@ptrCast(self.arena.arena[value_offset .. value_offset + value.len].ptr));
                node_to_update.*.value = value_ptr[0..value.len];
                @memcpy(node_to_update.*.value.?, value);
                return;
            } else {
                if (!created) {
                    new_node_offset = try self.arena.reserve(@sizeOf(Node) + (new_node_height + 1) * @sizeOf(u64));
                    new_node = @alignCast(@ptrCast(&self.arena.arena[new_node_offset]));
                    const new_node_tower_start: u64 = new_node_offset + @sizeOf(Node);
                    const new_node_tower_end: u64 = new_node_tower_start + @sizeOf(u64) * (new_node_height + 1);
                    const new_node_tower_ptr: [*]u64 = @constCast(@alignCast(@ptrCast(self.arena.arena[new_node_tower_start..new_node_tower_end].ptr)));

                    const key_value_offset = try self.arena.reserve(key.len + value.len);
                    const key_ptr: [*]u8 = @alignCast(@ptrCast(self.arena.arena[key_value_offset .. key_value_offset + key.len].ptr));
                    const value_ptr: [*]u8 = @alignCast(@ptrCast(self.arena.arena[key_value_offset + key.len .. key_value_offset + key.len + value.len].ptr));

                    new_node.?.* = .{
                        .key = key_ptr[0..key.len],
                        .value = value_ptr[0..value.len],
                        .tower = new_node_tower_ptr[0 .. new_node_height + 1],
                    };

                    @memcpy(new_node.?.*.key.?, key);
                    @memcpy(new_node.?.*.value.?, value);
                    created = true;
                    _ = @atomicRmw(u32, &self.size, .Add, 1, .seq_cst);
                }

                for (0..new_node_height + 1) |i| {
                    new_node.?.tower[i] = successors[i];
                }

                var pred: *Node = @alignCast(@ptrCast(&self.arena.arena[predecessors[0]]));
                if (@cmpxchgWeak(u64, &pred.tower[0], successors[0], new_node_offset, .seq_cst, .seq_cst) != null) {
                    continue;
                }

                if (predecessors.len > 1) {
                    for (1..new_node_height + 1) |i| {
                        while (true) {
                            pred = @alignCast(@ptrCast(&self.arena.arena[predecessors[i]]));
                            if (@cmpxchgWeak(u64, &pred.tower[i], successors[i], new_node_offset, .seq_cst, .seq_cst) == null) {
                                break;
                            }
                            _ = self.search(key, predecessors, successors);
                        }
                    }
                }

                return;
            }
        }
    }

    pub fn find(self: *Memtable, key: BinaryData) ?BinaryData {
        const result = self.search(key, null, null);
        if (result == 0) {
            return null;
        }
        const node: *Node = @alignCast(@ptrCast(&self.arena.arena[result]));
        return node.value;
    }

    pub inline fn canAdd(self: *const Memtable, data_size: u64) bool {
        const current_offset: u64 = self.arena.currentOffset();
        const node_size: u64 = @sizeOf(Node) + @sizeOf(u64) * self.max_level;
        var new_node_offset: u64 = ((current_offset + node_size) + @alignOf(usize) - 1) & ~@as(u64, (@alignOf(usize) - 1));
        new_node_offset += data_size;
        new_node_offset = (new_node_offset + @alignOf(usize) - 1) & ~@as(u64, (@alignOf(usize) - 1));
        return new_node_offset <= self.max_size;
    }

    pub inline fn iterator(self: *const Memtable) MemtableIterator {
        const head: *Node = @alignCast(@ptrCast(&self.arena.arena[0]));
        return MemtableIterator{
            .arena = &self.arena,
            .current = @alignCast(@ptrCast(&self.arena.arena[head.tower[0]])),
        };
    }

    inline fn pickLevel(self: *const Memtable) u8 {
        return @min(self.max_level, @as(u8, @intCast(@ctz(self.rng.int(u32) & ((@as(u32, 1) << @intCast(self.max_level)) - 1)) + 1)));
    }

    fn search(self: *Memtable, key: BinaryData, predecessors: ?[]u64, successors: ?[]u64) u64 {
        var pred: ?*Node = null;
        var pred_offset: u64 = 0;
        var curr: ?*Node = null;
        var curr_offset: u64 = 0;
        var succ_offset: u64 = 0;

        var level: i8 = @as(i8, @intCast(self.max_level));
        while (level >= 0) : (level -= 1) {
            const l: u64 = @intCast(level);
            pred = @alignCast(@ptrCast(&self.arena.arena[pred_offset]));
            curr_offset = @atomicLoad(u64, &pred.?.tower[l], .seq_cst);
            if (curr_offset == 0) continue;

            curr = @alignCast(@ptrCast(&self.arena.arena[curr_offset]));
            while (true) {
                succ_offset = @atomicLoad(u64, &curr.?.tower[l], .seq_cst);
                if (succ_offset != 0 and self.compare_fn(curr.?.key.?, key) < 0) {
                    pred = curr;
                    pred_offset = curr_offset;
                    curr_offset = succ_offset;
                    curr = @alignCast(@ptrCast(&self.arena.arena[succ_offset]));
                } else {
                    break;
                }
            }

            if (predecessors != null and successors != null) {
                predecessors.?[l] = pred_offset;
                successors.?[l] = curr_offset;
            }
        }

        if (curr != null and curr.?.key != null and self.compare_fn(curr.?.key.?, key) == 0) {
            return curr_offset;
        }

        return 0;
    }
};

// Tests
const testing = std.testing;

fn visualizeMemtable(memtable: *Memtable) void {
    std.debug.print("Memtable, current_offset = {d}\n", .{memtable.arena.currentOffset()});
    var node_offset: u64 = 0;
    while (node_offset < memtable.max_size) {
        const curr: *Memtable.Node = @alignCast(@ptrCast(&memtable.arena.arena[node_offset]));
        if (curr.key) |key| {
            std.debug.print("{any} (size: {d}):\t", .{ key, key.len });
        } else {
            std.debug.print("{any} (size: 0):\t", .{null});
        }
        for (curr.tower) |j| {
            std.debug.print("{d}\t", .{j});
        }
        std.debug.print("\n", .{});
        node_offset = curr.tower[0];
        if (node_offset == 0) {
            break;
        }
    }
}

test "Memtable#add" {
    var memtable = try Memtable.init(
        testing.allocator,
        std.crypto.random,
        8192,
        8,
        "./",
    );

    const k1: [1]u8 = utils.intToBytes(u8, @as(u8, @intCast(0)));
    const v1: [1]u8 = utils.intToBytes(u8, @as(u8, @intCast(0)));
    const v2: [1]u8 = utils.intToBytes(u8, @as(u8, @intCast(11)));
    try memtable.add(&k1, &v1);
    try testing.expect(memtable.size == 1);
    try memtable.add(&k1, &v2);
    try testing.expect(memtable.size == 1);

    var test_value: [1]u8 = undefined;
    var test_key: [1]u8 = undefined;
    for (1..10) |i| {
        test_key = utils.intToBytes(u8, @as(u8, @intCast(i)));
        test_value = utils.intToBytes(u8, @as(u8, @intCast(i)));
        try memtable.add(&test_key, &test_value);
        // visualizeMemtable(&memtable);
        // std.debug.print("\n", .{});
    }
    try testing.expect(memtable.size == 10);
    // visualizeMemtable(&memtable);

    for (1..10) |i| {
        test_key = utils.intToBytes(u8, @as(u8, @intCast(10 - i)));
        const value = memtable.find(&test_key);
        try testing.expect(std.mem.eql(u8, value.?, test_key[0..]));
    }

    var iterator = memtable.iterator();
    var i: u64 = 0;
    while (iterator.next()) |record| : (i += 1) {
        test_key = utils.intToBytes(u8, @as(u8, @intCast(i)));
        if (i == 0) {
            test_value = utils.intToBytes(u8, @as(u8, @intCast(11)));
        } else {
            test_value = utils.intToBytes(u8, @as(u8, @intCast(i)));
        }
        try testing.expect(std.mem.eql(u8, record.key, test_key[0..]));
        try testing.expect(std.mem.eql(u8, record.value, test_value[0..]));
    }

    try memtable.wal.deleteFile();
    memtable.destroy();
}

test "Arena" {
    var arena = try Arena.init(testing.allocator, 104);
    defer arena.deinit();

    var o: u64 = try arena.reserve(@sizeOf(u64));
    try testing.expect(o == 0);
    o = try arena.reserve(@sizeOf(u128));
    try testing.expect(o == 8);
    o = try arena.reserve(@sizeOf(u64));
    try testing.expect(o == 24);
    o = try arena.reserve(@sizeOf(u128));
    try testing.expect(o == 32);
    o = try arena.reserve(@sizeOf(u64));
    try testing.expect(o == 48);
    o = try arena.reserve(@sizeOf(u128));
    try testing.expect(o == 56);
    o = try arena.reserve(@sizeOf(u64));
    try testing.expect(o == 72);
    o = try arena.reserve(@sizeOf(u64));
    try testing.expect(o == 80);
    o = try arena.reserve(@sizeOf(u128));
    try testing.expect(o == 88);
}

// fn allocTestJob(arena: *Arena, thread_number: usize) void {
//     var operation: u8 = 0;
//     const max_iteration = 32;
//     for (0..max_iteration) |_| {
//         if (operation == 0) {
//             const data: [128]u8 = [_]u8{'C'} ** 128;
//             const offset = try arena.reserve(128);
//             const data_slot: *[128]u8 = @alignCast(@ptrCast(&arena.arena[offset]));
//             @memcpy(data_slot, data[0..]);
//         } else {
//             const data: *const [16:0]u8 = "AAAAAAAABBBBBBBB";
//             const offset = try arena.reserve(16);
//             const data_slot: *[16:0]u8 = @alignCast(@ptrCast(&arena.arena[offset]));
//             @memcpy(data_slot, data[0..]);
//         }
//         operation = (operation + 1) % 2;
//     }
//     std.debug.print("[TEST] Thread {d} done\n", .{thread_number});
// }

// test "Arena concurrency" {
//     var arena = try Arena.init(testing.allocator, 69632);
//     defer arena.deinit();

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, allocTestJob, .{ &arena, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
//     std.debug.print("All work is done!\n", .{});
//     var file = try std.fs.cwd().createFile("output", .{ .read = true, .truncate = true });
//     defer file.close();
//     try file.writeAll(arena.arena);
// }

// fn memtableTestJob(memtable: *Memtable, thread_number: usize) void {
//     var inserted_numbers = std.ArrayList(u64).init(testing.allocator);
//     defer inserted_numbers.deinit();

//     var operation: u8 = 0;
//     const max_iteration = 32;
//     for (0..max_iteration) |i| {
//         if (operation == 0) {
//             const data: u64 = thread_number * max_iteration + i;
//             const key: [8]u8 = utils.intToBytes(u64, data);
//             const value: [8]u8 = utils.intToBytes(u64, data);
//             inserted_numbers.append(data) catch unreachable;
//             memtable.add(&key, &value) catch |e| {
//                 std.debug.print("{any}\n", .{e});
//             };
//         } else {
//             const data: u64 = inserted_numbers.orderedRemove(0);
//             const key: [8]u8 = utils.intToBytes(u64, data);
//             const result = memtable.find(&key);
//             if (result == null) {
//                 std.debug.print("Error: key = {d} not found\n", .{data});
//             }
//         }
//         operation = (operation + 1) % 2;
//     }
//     std.debug.print("[TEST] Thread {d} done\n", .{thread_number});
// }

// test "Memtable concurrency" {
//     var memtable = try Memtable.init(
//         testing.allocator,
//         std.crypto.random,
//         69632,
//         8,
//         "./",
//     );

//     const start = try std.time.Instant.now();
//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, memtableTestJob, .{ &memtable, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }

//     const end = try std.time.Instant.now();
//     const elapsed: f64 = @floatFromInt(end.since(start));
//     std.debug.print("All work is done! Time: {d:.3} ms\n", .{elapsed / std.time.ns_per_ms});

//     // visualizeMemtable(&memtable);
//     try memtable.wal.deleteFile();
//     memtable.destroy();
// }
