const std = @import("std");

const Memtable = @import("./memtable.zig").Memtable;

pub const MemtableList = struct {
    allocator: std.mem.Allocator,
    head: *Node,

    pub const Node = struct {
        id: u64,
        memtable: ?*Memtable,
        next: ?*Node,
        next_perm: ?*Node, // TODO: use vector to hold both links
        delete_flag: std.atomic.Value(bool),
        use_counter: u32,
        padding: u8 align(std.atomic.cache_line) = 0,
    };

    const Iterator = struct {
        current: *Node,

        pub fn init(head: *Node) Iterator {
            _ = @atomicRmw(u32, &head.use_counter, .Add, 1, .seq_cst);
            return .{ .current = head };
        }

        pub fn deinit(self: *Iterator) void {
            _ = @atomicRmw(u32, &self.current.use_counter, .Sub, 1, .seq_cst);
        }

        pub inline fn next(self: *Iterator) ?*Node {
            var pointer = @atomicLoad(?*Node, &self.current.next, .acquire);

            while (pointer) |ptr| {
                _ = @atomicRmw(u32, &ptr.use_counter, .Add, 1, .seq_cst);
                if (ptr.delete_flag.load(.seq_cst) == true) {
                    pointer = ptr.next;
                    _ = @atomicRmw(u32, &ptr.use_counter, .Sub, 1, .seq_cst);
                } else {
                    break;
                }
            }

            if (pointer) |ptr| {
                const result = ptr;
                _ = @atomicRmw(u32, &self.current.use_counter, .Sub, 1, .seq_cst);
                self.current = ptr;
                return result;
            }

            return null;
        }
    };

    pub fn init(allocator: std.mem.Allocator) !MemtableList {
        const sentinel_node = try allocator.create(Node);
        sentinel_node.* = Node{
            .id = 0,
            .memtable = null,
            .next = null,
            .next_perm = null,
            .delete_flag = std.atomic.Value(bool).init(false),
            .use_counter = 0,
        };

        return .{
            .allocator = allocator,
            .head = sentinel_node,
        };
    }

    pub fn deinit(self: *MemtableList) void {
        var pointer: ?*Node = self.head;
        while (pointer) |p| {
            pointer = p.next_perm;
            self.allocator.destroy(p);
        }
    }

    pub fn prepend(self: *MemtableList, id: u64, memtable: *Memtable) !void {
        var node = try self.allocator.create(Node);
        node.* = Node{
            .id = id,
            .memtable = memtable,
            .next = null,
            .next_perm = null, // TODO: use SIMD
            .delete_flag = std.atomic.Value(bool).init(false),
            .use_counter = 0,
        };

        var old_head = @atomicLoad(?*Node, &self.head.next, .seq_cst);
        node.next = old_head;
        node.next_perm = @atomicLoad(?*Node, &self.head.next_perm, .seq_cst);

        while (@cmpxchgWeak(?*Node, &self.head.next, old_head, node, .seq_cst, .seq_cst) != null) {
            old_head = @atomicLoad(?*Node, &self.head.next, .seq_cst);
            node.next = old_head;
            node.next_perm = @atomicLoad(?*Node, &self.head.next_perm, .seq_cst);
        }
        @atomicStore(?*Node, &self.head.next_perm, self.head.next, .seq_cst);
    }

    pub fn mark_delete(self: *MemtableList, id: u64) bool {
        var prev: *Node = self.head;
        _ = @atomicRmw(u32, &prev.use_counter, .Add, 1, .seq_cst);

        var current = @atomicLoad(?*Node, &prev.next, .seq_cst);
        while (current) |curr| {
            _ = @atomicRmw(u32, &curr.use_counter, .Add, 1, .seq_cst);
            if (curr.id == id) {
                curr.delete_flag.store(true, .seq_cst);
                while (@cmpxchgWeak(?*Node, &prev.next, curr, curr.next, .seq_cst, .seq_cst) != null) {
                    _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
                    prev = self.head;
                    _ = @atomicRmw(u32, &prev.use_counter, .Add, 1, .seq_cst);
                    while (prev.next != curr and prev.next_perm != null) {
                        const tmp = prev;
                        prev = @atomicLoad(*Node, &prev.next_perm.?, .seq_cst);
                        _ = @atomicRmw(u32, &tmp.use_counter, .Sub, 1, .seq_cst);
                        _ = @atomicRmw(u32, &prev.use_counter, .Add, 1, .seq_cst);
                    }
                }

                _ = @atomicRmw(u32, &curr.use_counter, .Sub, 1, .seq_cst);
                _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
                return true;
            }

            _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
            prev = curr;
            current = @atomicLoad(?*Node, &prev.next, .seq_cst);
        }

        _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
        return false;
    }

    pub fn delete(self: *MemtableList, id: u64) ?*Memtable {
        var prev: *Node = self.head;
        _ = @atomicRmw(u32, &prev.use_counter, .Add, 1, .seq_cst);

        var current = @atomicLoad(?*Node, &prev.next_perm, .seq_cst);
        while (current) |curr| {
            _ = @atomicRmw(u32, &curr.use_counter, .Add, 1, .seq_cst);
            if (curr.id == id) {
                if (curr.delete_flag.load(.seq_cst) == false) {
                    _ = @atomicRmw(u32, &curr.use_counter, .Sub, 1, .seq_cst);
                    _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
                    return null;
                }
                if (@atomicLoad(u32, &curr.use_counter, .seq_cst) != 1) {
                    _ = @atomicRmw(u32, &curr.use_counter, .Sub, 1, .seq_cst);
                    _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
                    return null;
                }

                while (@cmpxchgWeak(?*Node, &prev.next_perm, curr, curr.next_perm, .seq_cst, .seq_cst) != null) {
                    _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
                    prev = self.head;
                    _ = @atomicRmw(u32, &prev.use_counter, .Add, 1, .seq_cst);
                    while (prev.next_perm != curr and prev.next_perm != null) {
                        const tmp = prev;
                        prev = @atomicLoad(*Node, &prev.next_perm.?, .seq_cst);
                        _ = @atomicRmw(u32, &tmp.use_counter, .Sub, 1, .seq_cst);
                        _ = @atomicRmw(u32, &prev.use_counter, .Add, 1, .seq_cst);
                    }
                }

                _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
                const result = curr.memtable;
                self.allocator.destroy(curr);
                return result;
            }

            _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
            prev = curr;
            current = @atomicLoad(?*Node, &prev.next_perm, .seq_cst);
        }

        _ = @atomicRmw(u32, &prev.use_counter, .Sub, 1, .seq_cst);
        return null;
    }

    pub inline fn iterator(self: *const MemtableList) Iterator {
        return Iterator.init(self.head);
    }
};

// Testing
const testing = std.testing;

test "MemtableList#prepend" {
    var memtable = try Memtable.init(testing.allocator, std.crypto.random, 1, 1, 0.1, "./");

    var list = try MemtableList.init(testing.allocator);
    defer list.deinit();

    try list.prepend(0, &memtable);
    try testing.expect(list.head.next.?.id == 0);

    try list.prepend(1, &memtable);
    try testing.expect(list.head.next.?.id == 1);
    try testing.expect(list.head.next.?.next.?.id == 0);

    try memtable.wal.delete_file();
    memtable.destroy();
}

test "MemtableList#mark_delete" {
    var memtable = try Memtable.init(testing.allocator, std.crypto.random, 1, 1, 0.1, "./");

    var list = try MemtableList.init(testing.allocator);
    defer list.deinit();

    try list.prepend(0, &memtable);
    try list.prepend(1, &memtable);
    try list.prepend(2, &memtable);

    try testing.expect(list.mark_delete(0));
    try testing.expect(list.head.next.?.next.?.next == null);
    try testing.expect(list.head.next.?.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.next_perm.?.id == 0);
    try testing.expect(list.head.next.?.next.?.next_perm.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.next_perm.?.delete_flag.load(.acquire) == true);

    try memtable.wal.delete_file();
    memtable.destroy();
}

test "MemtableList#delete" {
    var memtable = try Memtable.init(testing.allocator, std.crypto.random, 1, 1, 0.1, "./");

    var list = try MemtableList.init(testing.allocator);
    defer list.deinit();

    try list.prepend(0, &memtable);
    try list.prepend(1, &memtable);
    try list.prepend(2, &memtable);

    try testing.expect(list.mark_delete(0));

    try testing.expect(list.head.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.next_perm.?.use_counter == 0);

    _ = list.delete(0);
    try testing.expect(list.head.next.?.next.?.next_perm == null);
    try testing.expect(list.head.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.use_counter == 0);

    _ = list.mark_delete(1);
    _ = list.mark_delete(2);
    _ = list.delete(2);
    _ = list.delete(1);
    try testing.expect(list.head.next == null);

    try memtable.wal.delete_file();
    memtable.destroy();
}

test "MemtableList Iterator" {
    var memtable = try Memtable.init(testing.allocator, std.crypto.random, 1, 1, 0.1, "./");

    var list = try MemtableList.init(testing.allocator);
    defer list.deinit();

    try list.prepend(0, &memtable);
    try list.prepend(1, &memtable);
    try list.prepend(2, &memtable);
    try list.prepend(3, &memtable);

    var iter = list.iterator();
    while (iter.next()) |r| {
        try testing.expect(r.memtable == &memtable);
        try testing.expect(iter.current.use_counter == 1);
    }
    iter.deinit();

    try testing.expect(list.head.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.next.?.use_counter == 0);
    try testing.expect(list.head.next.?.next.?.next.?.next.?.use_counter == 0);

    try memtable.wal.delete_file();
    memtable.destroy();
}

// fn test_job(list: *MemtableList, memtable: *Memtable) void {
//     var active_ids = std.ArrayList(u64).init(testing.allocator);
//     defer active_ids.deinit();

//     var marked_delete_ids = std.ArrayList(u64).init(testing.allocator);
//     defer marked_delete_ids.deinit();

//     for (0..100000) |i| {
//         if (i % 1000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         const operation = std.crypto.random.intRangeAtMost(u8, 0, 100);
//         if (operation == 0) {
//             const id = std.crypto.random.int(u64);
//             active_ids.append(id) catch continue;
//             _ = list.prepend(id, memtable) catch {};
//         } else if (operation == 1) {
//             const id = active_ids.getLastOrNull();
//             if (id) |_| {
//                 _ = list.mark_delete(id.?);
//                 marked_delete_ids.append(id.?) catch {};
//             }
//         } else if (operation == 2) {
//             const id = marked_delete_ids.getLastOrNull();
//             if (id) |_| {
//                 _ = list.delete(id.?);
//             }
//         } else {
//             var iter = list.iterator();
//             defer iter.deinit();

//             while (iter.next()) |m| {
//                 _ = m;
//             }
//         }
//     }
// }

// test "MemtableList concurrecny" {
//     var memtable = try Memtable.init(testing.allocator, std.crypto.random, 1, 1, 0.1, "./");

//     var list = try MemtableList.init(testing.allocator);
//     defer list.deinit();

//     var threads: [4]std.Thread = undefined;
//     for (0..4) |i| {
//         threads[i] = try std.Thread.spawn(.{}, test_job, .{ &list, &memtable });
//     }

//     for (threads) |t| {
//         t.join();
//     }

//     try memtable.wal.delete_file();
//     memtable.destroy();
// }
