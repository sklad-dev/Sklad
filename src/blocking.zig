const std = @import("std");

const ApplicationError = @import("./constants.zig").ApplicationError;

const try_lock_for = @import("./utils.zig").try_lock_for;

pub fn AppendDeleteList(E: type, C: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: *Node,
        node_cleanup_fn: NodeCleanupFn,

        const NodeCleanupFn = *const fn (allocator: std.mem.Allocator, data_ptr: *E) void;
        pub const Condition = *const fn (entry: ?*E, expected: C) bool;

        pub const Node = struct {
            entry: ?*E,
            next: ?*Node,
            lock: std.Thread.Mutex = .{},
        };

        const Iterator = struct {
            current: *Node,

            pub fn init(head: *Node) !Iterator {
                if (!try_lock_for(&head.lock, 200)) return ApplicationError.ExecutionTimeout;
                return .{ .current = head };
            }

            pub fn deinit(self: *Iterator) void {
                self.current.lock.unlock();
            }

            pub inline fn next(self: *Iterator) !?*Node {
                var curr: ?*Node = undefined;
                if (self.current.next) |n| {
                    curr = n;
                    if (!try_lock_for(&curr.?.lock, 200)) {
                        return ApplicationError.ExecutionTimeout;
                    }

                    const prev = self.current;
                    self.current = curr.?;
                    prev.lock.unlock();
                    return self.current;
                }
                return null;
            }
        };

        pub fn init(allocator: std.mem.Allocator, node_cleanup_fn: NodeCleanupFn) !Self {
            const sentinel_node = try allocator.create(Node);
            sentinel_node.* = Node{
                .entry = null,
                .next = null,
            };

            return .{
                .allocator = allocator,
                .head = sentinel_node,
                .node_cleanup_fn = node_cleanup_fn,
            };
        }

        pub fn deinit(self: *Self) void {
            var pointer: ?*Node = self.head;
            while (pointer) |node| {
                if (node.entry) |e| {
                    self.node_cleanup_fn(self.allocator, e);
                }
                pointer = node.next;
                self.allocator.destroy(node);
            }
        }

        pub fn prepend(self: *Self, entry: *E) !bool {
            if (!try_lock_for(&self.head.lock, 200)) return false;
            if (self.head.next != null) {
                if (!try_lock_for(&self.head.next.?.lock, 200)) {
                    self.head.lock.unlock();
                    return false;
                }
            }

            var node = try self.allocator.create(Node);
            node.* = Node{
                .entry = entry,
                .next = null,
            };

            node.next = self.head.next;
            self.head.next = node;

            if (node.next != null) {
                node.next.?.lock.unlock();
            }
            self.head.lock.unlock();

            return true;
        }

        pub fn remove(self: *Self, condition: Condition, expected: C) bool {
            if (!try_lock_for(&self.head.lock, 200)) return false;
            var prev: *Node = self.head;
            var curr: ?*Node = undefined;
            if (prev.next) |n| {
                curr = n;
                while (curr != null) {
                    if (!try_lock_for(&curr.?.lock, 200)) {
                        prev.lock.unlock();
                        return false;
                    }

                    if (condition(curr.?.entry, expected)) {
                        prev.next = curr.?.next;
                        prev.lock.unlock();

                        if (curr.?.entry) |e| {
                            self.node_cleanup_fn(self.allocator, e);
                            curr.?.entry = null;
                        }
                        self.allocator.destroy(curr.?);
                        return true;
                    }

                    prev.lock.unlock();
                    prev = curr.?;
                    curr = curr.?.next;
                }
            } else {
                self.head.lock.unlock();
            }

            return false;
        }

        pub inline fn iterator(self: *const Self) !Iterator {
            return Iterator.init(self.head);
        }
    };
}

// Testing
const testing = std.testing;

fn test_cleanup(allocator: std.mem.Allocator, data: *u64) void {
    allocator.destroy(data);
}

fn test_condition(T: type, C: type) type {
    return struct {
        pub fn match(value: ?*T, expected: C) bool {
            return value != null and value.?.* == expected;
        }
    };
}

test "MemtableList#prepend" {
    var list = try AppendDeleteList(u64, u64).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const v1 = try list.allocator.create(u64);
    v1.* = 0;
    try testing.expect(try list.prepend(v1));
    try testing.expect(list.head.next.?.entry.?.* == 0);

    const v2 = try list.allocator.create(u64);
    v2.* = 1;
    try testing.expect(try list.prepend(v2));
    try testing.expect(list.head.next.?.entry.?.* == 1);
    try testing.expect(list.head.next.?.next.?.entry.?.* == 0);
}

test "MemtableList#remove" {
    var list = try AppendDeleteList(u64, u64).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const v1 = try list.allocator.create(u64);
    v1.* = 0;
    try testing.expect(try list.prepend(v1));
    const v2 = try list.allocator.create(u64);
    v2.* = 1;
    try testing.expect(try list.prepend(v2));
    const v3 = try list.allocator.create(u64);
    v3.* = 2;
    try testing.expect(try list.prepend(v3));
    const v4 = try list.allocator.create(u64);
    v4.* = 3;
    try testing.expect(try list.prepend(v4));

    _ = list.remove(test_condition(u64, u64).match, 1);
    try testing.expect(list.head.next.?.next.?.next.?.entry.?.* == 0);

    _ = list.remove(test_condition(u64, u64).match, 0);
    try testing.expect(list.head.next.?.next.?.next == null);

    _ = list.remove(test_condition(u64, u64).match, 3);
    _ = list.remove(test_condition(u64, u64).match, 2);
    try testing.expect(list.head.next == null);
}

test "MemtableList Iterator" {
    var list = try AppendDeleteList(u64, u64).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const v1 = try list.allocator.create(u64);
    v1.* = 0;
    try testing.expect(try list.prepend(v1));
    const v2 = try list.allocator.create(u64);
    v2.* = 1;
    try testing.expect(try list.prepend(v2));
    const v3 = try list.allocator.create(u64);
    v3.* = 2;
    try testing.expect(try list.prepend(v3));
    const v4 = try list.allocator.create(u64);
    v4.* = 3;
    try testing.expect(try list.prepend(v4));

    var iter = try list.iterator();
    // defer iter.deinit();

    var result = try iter.next();
    try testing.expect(result.?.entry.?.* == 3);
    result = try iter.next();
    try testing.expect(result.?.entry.?.* == 2);
    result = try iter.next();
    try testing.expect(result.?.entry.?.* == 1);
    result = try iter.next();
    try testing.expect(result.?.entry.?.* == 0);
    result = try iter.next();
    try testing.expect(result == null);

    iter.deinit();
    iter = try list.iterator();
    result = try iter.next();
    try testing.expect(result.?.entry.?.* == 3);
}

// fn test_job(list: *AppendDeleteList(u64, u64), thread_number: usize) void {
//     var ids = std.ArrayList(u64).init(testing.allocator);
//     defer ids.deinit();

//     var operation: u8 = 0;
//     const max_iteration = 625000;
//     for (0..max_iteration) |i| {
//         if ((i % 5000 == 0) or (i > 624500 and i % 100 == 0)) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         if (operation == 0) {
//             const data = list.allocator.create(u64) catch continue;
//             data.* = max_iteration * thread_number + i;
//             ids.append(data.*) catch continue;
//             _ = list.prepend(data) catch continue;
//         } else if (operation >= 1 and operation < 9) {
//             var iter = list.iterator() catch continue;
//             defer iter.deinit();

//             var counter: u8 = 0;
//             while (true) {
//                 _ = iter.next() catch continue;
//                 counter += 1;
//                 if (counter >= 10) break;
//             }
//         } else {
//             if (ids.items.len > 0) {
//                 const id = ids.orderedRemove(0);
//                 _ = list.remove(test_condition(u64, u64).match, id);
//             }
//         }
//         operation = (operation + 1) % 10;
//     }
// }

// test "MemtableList concurrecny" {
//     var list = try AppendDeleteList(u64, u64).init(testing.allocator, test_cleanup);
//     defer list.deinit();

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, test_job, .{ &list, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
// }
