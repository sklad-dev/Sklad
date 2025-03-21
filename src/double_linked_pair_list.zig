const std = @import("std");

pub fn DoubleLinkedPairList(K: type, V: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: ?*Pair,
        tail: ?*Pair,
        num_elements: u64,

        pub const Pair = struct {
            key: K,
            value: V,
            prev: ?*Pair,
            next: ?*Pair,
        };

        const Iterator = struct {
            current: ?*Pair,

            pub inline fn next(self: *Iterator) ?V {
                if (self.current) |c| {
                    const result = c.value;
                    self.current = c.next;
                    return result;
                } else {
                    return null;
                }
                return null;
            }
        };

        const ReverseIterator = struct {
            current: ?*Pair,

            pub inline fn next(self: *ReverseIterator) ?V {
                if (self.current) |c| {
                    const result = c.value;
                    self.current = c.prev;
                    return result;
                } else {
                    return null;
                }
                return null;
            }
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .head = null,
                .tail = null,
                .num_elements = 0,
            };
        }

        pub fn append(self: *Self, key: K, value: V) !void {
            const pair = try self.allocator.create(Pair);
            pair.* = .{
                .key = key,
                .value = value,
                .prev = self.tail,
                .next = null,
            };
            if (self.tail) |t| {
                t.next = pair;
            }
            if (self.head == null) {
                self.head = pair;
            }
            self.tail = pair;
            self.num_elements += 1;
        }

        pub fn take(self: *Self, key: K) ?V {
            var pointer = self.head;
            while (pointer != null and pointer.?.key != key) {
                pointer = pointer.?.next;
            }
            if (pointer) |p| {
                if (p.key == key) {
                    const result = pointer.?.value;
                    if (p.prev) |prev| {
                        prev.next = p.next;
                    } else {
                        self.head = p.next;
                    }
                    if (p.next) |next| {
                        next.prev = p.prev;
                    } else {
                        self.tail = p.prev;
                    }
                    self.allocator.destroy(p);
                    self.num_elements -= 1;
                    return result;
                }
            }
            return null;
        }

        pub fn peek(self: *Self, key: K) ?V {
            var pointer = self.head;
            while (pointer != null and pointer.?.key != key) {
                pointer = pointer.?.next;
            }
            if (pointer) |p| {
                if (p.key == key) {
                    return pointer.?.value;
                }
            }
            return null;
        }

        pub inline fn size(self: *Self) u64 {
            return self.num_elements;
        }

        pub inline fn iterator(self: *const Self) Iterator {
            return .{ .current = self.head };
        }

        pub inline fn reverse_iterator(self: *const Self) ReverseIterator {
            return .{ .current = self.tail };
        }

        pub fn deinit(self: *Self) void {
            var current = self.head;
            while (current) |c| {
                const tmp = c;
                current = tmp.next;
                self.allocator.destroy(tmp);
            }
        }
    };
}

// Tests
const testing = std.testing;

test "#append" {
    var list = DoubleLinkedPairList(i8, i8).init(testing.allocator);
    defer list.deinit();

    try testing.expect(list.head == null);
    try testing.expect(list.tail == null);

    try list.append(1, -1);
    try testing.expect(list.head.?.key == 1);
    try testing.expect(list.head.?.value == -1);
    try testing.expect(list.tail.?.key == 1);
    try testing.expect(list.tail.?.value == -1);

    try list.append(2, -2);
    try testing.expect(list.head.?.key == 1);
    try testing.expect(list.head.?.value == -1);
    try testing.expect(list.tail.?.key == 2);
    try testing.expect(list.tail.?.value == -2);

    try list.append(3, -3);
    try testing.expect(list.head.?.key == 1);
    try testing.expect(list.head.?.value == -1);
    try testing.expect(list.tail.?.key == 3);
    try testing.expect(list.tail.?.value == -3);

    try testing.expect(list.size() == 3);
}

test "#take" {
    var list = DoubleLinkedPairList(i8, i8).init(testing.allocator);
    defer list.deinit();

    try list.append(1, -1);
    try list.append(2, -2);
    try list.append(3, -3);
    try list.append(4, -4);
    try list.append(5, -5);

    const r1 = list.take(3);
    try testing.expect(r1 == -3);
    try testing.expect(list.size() == 4);

    const r2 = list.take(1);
    try testing.expect(r2 == -1);
    try testing.expect(list.head.?.key == 2);
    try testing.expect(list.head.?.value == -2);
    try testing.expect(list.size() == 3);

    const r3 = list.take(5);
    try testing.expect(r3 == -5);
    try testing.expect(list.tail.?.key == 4);
    try testing.expect(list.tail.?.value == -4);
    try testing.expect(list.size() == 2);

    _ = list.take(2);
    _ = list.take(4);
    try testing.expect(list.head == null);
    try testing.expect(list.tail == null);
    try testing.expect(list.size() == 0);
}

test "#peek" {
    var list = DoubleLinkedPairList(i8, i8).init(testing.allocator);
    defer list.deinit();

    try list.append(1, -1);
    try list.append(2, -2);
    try list.append(3, -3);

    const r1 = list.peek(3);
    try testing.expect(r1 == -3);
    try testing.expect(list.size() == 3);

    const r2 = list.peek(1);
    try testing.expect(r2 == -1);
    try testing.expect(list.size() == 3);
}

test "Iterator" {
    var list = DoubleLinkedPairList(usize, usize).init(testing.allocator);
    defer list.deinit();

    for (0..5) |i| {
        try list.append(i, i);
    }

    var iter = list.iterator();
    var expected: usize = 0;
    while (iter.next()) |value| {
        try testing.expect(value == expected);
        expected += 1;
    }
}
