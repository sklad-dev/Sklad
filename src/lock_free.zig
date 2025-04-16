const std = @import("std");

// S have to be power of 2 so it is possible to use bitwise and to compute modulo
pub fn DestroyBuffer(E: type, S: u64) type {
    return struct {
        head: u64,
        buffer: []?*E,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) Self {
            var buffer = allocator.alloc(?*E, S) catch unreachable;
            for (0..buffer.len) |i| {
                buffer[i] = null;
            }
            return .{
                .head = 0,
                .buffer = buffer,
            };
        }

        pub inline fn put(self: *Self, entry: *E) ?*E {
            const index = @atomicRmw(u64, &self.head, .Add, 1, .seq_cst);
            _ = @atomicRmw(u64, &self.head, .And, S - 1, .seq_cst);
            const to_delete = @atomicRmw(?*E, &self.buffer[index % S], .Xchg, entry, .seq_cst);
            return to_delete;
        }
    };
}

pub fn Queue(E: type, S: u64) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: *Node,
        tail: *Node,
        destroy_buffer: *DestroyBuffer(Node, S),

        const Node = struct {
            entry: ?E,
            next: ?*Node,
            prev: ?*Node,
            use_counter: u32,
            padding1: u8 align(std.atomic.cache_line) = 0,
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            var prev_guard = allocator.create(Node) catch unreachable;
            prev_guard.* = Node{
                .entry = null,
                .next = null,
                .prev = null,
                .use_counter = 0,
            };

            var start_guard = allocator.create(Node) catch unreachable;
            start_guard.* = Node{
                .entry = null,
                .next = null,
                .prev = null,
                .use_counter = 0,
            };

            start_guard.prev = prev_guard;
            prev_guard.next = start_guard;

            const destroy_buffer = allocator.create(DestroyBuffer(Node, S)) catch unreachable;
            destroy_buffer.* = DestroyBuffer(Node, S).init(allocator);

            return Self{
                .allocator = allocator,
                .head = start_guard,
                .tail = start_guard,
                .destroy_buffer = destroy_buffer,
            };
        }

        pub fn deinit(self: *Self) void {
            // TODO: THIS IS NOT THREAD SAFE!
            var head_pointer: ?*Node = self.head;
            if (head_pointer.?.prev) |prev_guard| {
                self.allocator.destroy(prev_guard);
            }
            while (head_pointer != null) {
                const next_node = head_pointer.?.next;
                self.allocator.destroy(head_pointer.?);
                head_pointer = next_node;
            }
            for (self.destroy_buffer.buffer) |node| {
                if (node) |n| self.allocator.destroy(n);
            }
            self.allocator.free(self.destroy_buffer.buffer);
            self.allocator.destroy(self.destroy_buffer);
        }

        pub fn enqueue(self: *Self, entry: E) void {
            const node = self.allocator.create(Node) catch unreachable;
            node.* = Node{
                .entry = entry,
                .next = null,
                .prev = null,
                .use_counter = 0,
            };

            while (true) {
                var ltail = @atomicLoad(*Node, &self.tail, .seq_cst);
                _ = @atomicRmw(u32, &ltail.use_counter, .Add, 1, .seq_cst);
                if (ltail != @atomicLoad(*Node, &self.tail, .seq_cst)) {
                    _ = @atomicRmw(u32, &ltail.use_counter, .Sub, 1, .seq_cst);
                    continue;
                }

                var lprev = @atomicLoad(*Node, &ltail.prev.?, .seq_cst);
                if (@atomicLoad(?*Node, &lprev.next, .seq_cst) == null) {
                    @atomicStore(?*Node, &lprev.next, ltail, .seq_cst);
                }
                node.prev = ltail;
                if (@cmpxchgWeak(
                    *Node,
                    &self.tail,
                    ltail,
                    node,
                    .seq_cst,
                    .seq_cst,
                ) == null) {
                    @atomicStore(?*Node, &ltail.next, node, .seq_cst);
                    _ = @atomicRmw(u32, &ltail.use_counter, .Sub, 1, .seq_cst);
                    return;
                }
                _ = @atomicRmw(u32, &ltail.use_counter, .Sub, 1, .seq_cst);
            }
        }

        pub fn dequeue(self: *Self) ?E {
            while (true) {
                const lhead = @atomicLoad(*Node, &self.head, .seq_cst);
                _ = @atomicRmw(u32, &lhead.use_counter, .Add, 1, .seq_cst);
                if (lhead != @atomicLoad(*Node, &self.head, .seq_cst)) {
                    _ = @atomicRmw(u32, &lhead.use_counter, .Sub, 1, .seq_cst);
                    continue;
                }

                const lnext = @atomicLoad(?*Node, &lhead.next, .seq_cst);
                if (lnext) |ln| {
                    if (@cmpxchgWeak(
                        *Node,
                        &self.head,
                        lhead,
                        ln,
                        .seq_cst,
                        .seq_cst,
                    ) == null) {
                        const entry = ln.entry;
                        _ = @atomicRmw(u32, &lhead.use_counter, .Sub, 1, .seq_cst);
                        if (self.destroy_buffer.put(lhead.prev.?)) |node| {
                            while (true) {
                                if (node.use_counter == 0) {
                                    self.allocator.destroy(node);
                                    break;
                                }
                            }
                        }
                        return entry;
                    }
                }
                _ = @atomicRmw(u32, &lhead.use_counter, .Sub, 1, .seq_cst);
            }
        }
    };
}

pub fn AppendDeleteList(E: type, C: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: MarkablePointer,
        destroy_buffer: *DestroyBuffer(Node, 256),
        node_cleanup_fn: NodeCleanupFn,

        const MarkablePointer = usize;
        const NodeCleanupFn = *const fn (allocator: std.mem.Allocator, data_ptr: *E) void;
        pub const Condition = *const fn (entry: ?*E, expected: C) bool;

        inline fn to_pure_pointer(markable: MarkablePointer) ?*Node {
            return @ptrFromInt(markable & ~@as(usize, 1));
        }

        inline fn to_marked_pointer(pointer: *Node, mark: bool) MarkablePointer {
            const address: usize = @intFromPtr(pointer);
            return address | @intFromBool(mark);
        }

        inline fn is_marked(markable: MarkablePointer) bool {
            return (markable & 1) == 1;
        }

        pub const Node = struct {
            entry: ?*E,
            next: MarkablePointer,
            next_perm: MarkablePointer,
            use_counter: u32,
            padding: u8 align(std.atomic.cache_line) = 0,
        };

        const Iterator = struct {
            previous: *Node,

            pub fn init(head: usize) Iterator {
                const head_ptr = to_pure_pointer(head).?;
                _ = @atomicRmw(u32, &head_ptr.use_counter, .Add, 1, .seq_cst);
                return .{ .previous = head_ptr };
            }

            pub fn deinit(self: *Iterator) void {
                _ = @atomicRmw(u32, &self.previous.use_counter, .Sub, 1, .seq_cst);
            }

            pub inline fn next(self: *Iterator) ?*Node {
                var curr: MarkablePointer = 0;
                var curr_ptr: ?*Node = null;
                var succ: MarkablePointer = 0;
                var tmp: MarkablePointer = 0;

                retry: while (true) {
                    curr = @atomicLoad(MarkablePointer, &self.previous.next, .seq_cst);
                    curr_ptr = to_pure_pointer(curr);
                    if (curr_ptr != null) {
                        while (curr_ptr != null and is_marked(curr)) {
                            _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Add, 1, .seq_cst);

                            succ = @atomicLoad(MarkablePointer, &curr_ptr.?.next, .seq_cst);
                            if (@cmpxchgWeak(MarkablePointer, &self.previous.next, curr, succ, .seq_cst, .seq_cst) != null) continue :retry;
                            tmp = curr;
                            curr = succ;
                            curr_ptr = to_pure_pointer(curr);

                            mark_next_perm: while (true) {
                                var prev: *Node = self.previous;
                                while (prev.next_perm != (tmp & ~@as(usize, 1))) {
                                    prev = to_pure_pointer(prev.next_perm) orelse continue :mark_next_perm;
                                }
                                @atomicStore(MarkablePointer, &prev.next_perm, tmp | @intFromBool(true), .seq_cst);
                                break;
                            }
                            _ = @atomicRmw(u32, &(to_pure_pointer(tmp).?.use_counter), .Sub, 1, .seq_cst);
                        }
                        if (curr_ptr == null) return null;

                        _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Add, 1, .seq_cst);
                        _ = @atomicRmw(u32, &self.previous.use_counter, .Sub, 1, .seq_cst);
                        self.previous = curr_ptr.?;
                        return curr_ptr.?;
                    }

                    return null;
                }
            }
        };

        pub fn init(allocator: std.mem.Allocator, node_cleanup_fn: NodeCleanupFn) !Self {
            const destroy_buffer = try allocator.create(DestroyBuffer(Node, 256));
            destroy_buffer.* = DestroyBuffer(Node, 256).init(allocator);

            const sentinel_node = try allocator.create(Node);
            sentinel_node.* = Node{
                .entry = null,
                .next = 0,
                .next_perm = 0,
                .use_counter = 0,
            };

            return .{
                .allocator = allocator,
                .head = to_marked_pointer(sentinel_node, false),
                .destroy_buffer = destroy_buffer,
                .node_cleanup_fn = node_cleanup_fn,
            };
        }

        // pub fn debug_print(self: *Self) void {
        //     std.debug.print("{d} [TEST] List structure:\n", .{std.Thread.getCurrentId()});
        //     var pure_pointer: ?*Node = to_pure_pointer(self.head).?;
        //     std.debug.print("{d} [TEST] Next sequence: ", .{std.Thread.getCurrentId()});
        //     while (pure_pointer) |pp| {
        //         std.debug.print("({d})[0x{X}]->[0x{X}], ", .{ std.Thread.getCurrentId(), to_marked_pointer(pp, false), pp.next });
        //         pure_pointer = to_pure_pointer(pp.next);
        //     }
        //     std.debug.print("||\n", .{});
        //     std.debug.print("{d} [TEST] Next perm sequence: ", .{std.Thread.getCurrentId()});
        //     pure_pointer = to_pure_pointer(self.head).?;
        //     while (pure_pointer) |pp| {
        //         std.debug.print("({d})[0x{X}]->[0x{X}], ", .{ std.Thread.getCurrentId(), to_marked_pointer(pp, false), pp.next_perm });
        //         pure_pointer = to_pure_pointer(pp.next_perm);
        //     }
        //     std.debug.print("||\n", .{});
        // }

        pub fn deinit(self: *Self) void {
            var pure_pointer: ?*Node = to_pure_pointer(self.head).?;
            while (pure_pointer) |pp| {
                if (pp.entry) |e| {
                    self.node_cleanup_fn(self.allocator, e);
                }
                pure_pointer = to_pure_pointer(pp.next_perm);
                self.allocator.destroy(pp);
            }
            for (self.destroy_buffer.buffer) |node| {
                if (node) |n| {
                    if (n.entry) |e| {
                        self.node_cleanup_fn(self.allocator, e);
                    }
                    self.allocator.destroy(n);
                }
            }
            self.allocator.free(self.destroy_buffer.buffer);
            self.allocator.destroy(self.destroy_buffer);
        }

        pub fn prepend(self: *Self, entry: *E) !void {
            var node = try self.allocator.create(Node);
            node.* = Node{
                .entry = entry,
                .next = 0,
                .next_perm = 0,
                .use_counter = 0,
            };

            const sent_ptr = to_pure_pointer(self.head).?;

            var curr: MarkablePointer = 0;
            var curr_ptr: ?*Node = null;
            var succ: MarkablePointer = 0;
            var tmp: MarkablePointer = 0;

            retry: while (true) {
                curr = @atomicLoad(MarkablePointer, &sent_ptr.next, .seq_cst);
                curr_ptr = to_pure_pointer(curr);
                if (curr_ptr != null) {
                    while (curr_ptr != null and is_marked(curr)) {
                        _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Add, 1, .seq_cst);

                        succ = @atomicLoad(MarkablePointer, &curr_ptr.?.next, .seq_cst);
                        if (@cmpxchgWeak(MarkablePointer, &sent_ptr.next, curr, succ, .seq_cst, .seq_cst) != null) continue :retry;
                        tmp = curr;
                        curr = succ;
                        curr_ptr = to_pure_pointer(curr);

                        mark_next_perm: while (true) {
                            var prev: *Node = sent_ptr;
                            while (prev.next_perm != (tmp & ~@as(usize, 1))) {
                                prev = to_pure_pointer(prev.next_perm) orelse continue :mark_next_perm;
                            }
                            @atomicStore(MarkablePointer, &prev.next_perm, tmp | @intFromBool(true), .seq_cst);
                            break;
                        }
                        _ = @atomicRmw(u32, &(to_pure_pointer(tmp).?.use_counter), .Sub, 1, .seq_cst);
                    }
                }

                node.next = curr;
                node.next_perm = @atomicLoad(MarkablePointer, &sent_ptr.next_perm, .seq_cst);
                if (@cmpxchgWeak(MarkablePointer, &sent_ptr.next, node.next, to_marked_pointer(node, false), .seq_cst, .seq_cst) != null) {
                    continue;
                }

                retry_nex_perm: while (true) {
                    if (@cmpxchgWeak(MarkablePointer, &sent_ptr.next_perm, node.next_perm, to_marked_pointer(node, false), .seq_cst, .seq_cst) != null) {
                        node.next_perm = @atomicLoad(MarkablePointer, &sent_ptr.next_perm, .seq_cst);
                        continue :retry_nex_perm;
                    }
                    break;
                }
                break;
            }
        }

        pub fn mark_delete(self: *Self, condition: Condition, expected: C) void {
            var prev_ptr: *Node = to_pure_pointer(self.head).?;
            var curr: MarkablePointer = 0;
            var curr_ptr: ?*Node = null;
            var succ: MarkablePointer = 0;
            var tmp: MarkablePointer = 0;

            retry: while (true) {
                prev_ptr = to_pure_pointer(self.head).?;
                while (true) {
                    curr = @atomicLoad(MarkablePointer, &prev_ptr.next, .seq_cst);
                    curr_ptr = to_pure_pointer(curr);
                    if (curr_ptr == null) return;

                    while (curr_ptr != null and is_marked(curr)) {
                        _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Add, 1, .seq_cst);

                        succ = @atomicLoad(MarkablePointer, &curr_ptr.?.next, .seq_cst);
                        if (@cmpxchgWeak(MarkablePointer, &prev_ptr.next, curr, succ, .seq_cst, .seq_cst) != null) {
                            continue :retry;
                        }
                        tmp = curr;
                        curr = succ;
                        curr_ptr = to_pure_pointer(curr);

                        mark_next_perm: while (true) {
                            var prev: *Node = prev_ptr;
                            while (prev.next_perm != (tmp & ~@as(usize, 1))) {
                                prev = to_pure_pointer(prev.next_perm) orelse continue :mark_next_perm;
                            }
                            @atomicStore(MarkablePointer, &prev.next_perm, tmp | @intFromBool(true), .seq_cst);
                            break;
                        }
                        _ = @atomicRmw(u32, &(to_pure_pointer(tmp).?.use_counter), .Sub, 1, .seq_cst);
                    }
                    if (curr_ptr == null) return;

                    if (condition(curr_ptr.?.entry, expected)) {
                        const marked = to_marked_pointer(curr_ptr.?, true);
                        if (@cmpxchgWeak(MarkablePointer, &prev_ptr.next, curr, marked, .seq_cst, .seq_cst) != null) {
                            continue :retry;
                        }
                        return;
                    }

                    prev_ptr = curr_ptr.?;
                }
            }
        }

        pub fn delete(self: *Self, condition: Condition, expected: C) bool {
            var prev_ptr: *Node = to_pure_pointer(self.head).?;
            _ = @atomicRmw(u32, &prev_ptr.use_counter, .Add, 1, .seq_cst);
            var curr: MarkablePointer = 0;
            var curr_ptr: ?*Node = null;
            retry: while (true) {
                prev_ptr = to_pure_pointer(self.head).?;
                while (true) {
                    curr = @atomicLoad(MarkablePointer, &prev_ptr.next_perm, .seq_cst);
                    curr_ptr = to_pure_pointer(curr);
                    if (curr_ptr != null) {
                        _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Add, 1, .seq_cst);
                        if (condition(curr_ptr.?.entry, expected)) {
                            if (is_marked(curr) and curr_ptr.?.use_counter == 1) {
                                if (@cmpxchgWeak(MarkablePointer, &prev_ptr.next_perm, curr, curr_ptr.?.next_perm, .seq_cst, .seq_cst) != null) {
                                    _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Sub, 1, .seq_cst);
                                    continue :retry;
                                }
                                _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Sub, 1, .seq_cst);
                                while (self.destroy_buffer.put(curr_ptr.?)) |node| {
                                    if (@cmpxchgWeak(u32, &node.use_counter, 0, 0, .seq_cst, .seq_cst) != null) {
                                        curr_ptr = node;
                                        continue;
                                    }
                                    if (node.entry) |e| {
                                        self.node_cleanup_fn(self.allocator, e);
                                        node.entry = null;
                                    }
                                    self.allocator.destroy(node);
                                    break;
                                }
                                return true;
                            } else {
                                _ = @atomicRmw(u32, &curr_ptr.?.use_counter, .Sub, 1, .seq_cst);
                                _ = @atomicRmw(u32, &prev_ptr.use_counter, .Sub, 1, .seq_cst);
                                return false;
                            }
                        } else {
                            _ = @atomicRmw(u32, &prev_ptr.use_counter, .Sub, 1, .seq_cst);
                            prev_ptr = curr_ptr.?;
                        }
                    } else {
                        _ = @atomicRmw(u32, &prev_ptr.use_counter, .Sub, 1, .seq_cst);
                        return false;
                    }
                }
            }
            return false;
        }

        pub inline fn iterator(self: *Self) Iterator {
            return Iterator.init(self.head);
        }
    };
}

// Testing
const testing = std.testing;

fn test_cleanup(allocator: std.mem.Allocator, data: *u8) void {
    allocator.destroy(data);
}

fn test_cleanup_u64(allocator: std.mem.Allocator, data: *u64) void {
    allocator.destroy(data);
}

inline fn to_pointer(p: usize) ?*AppendDeleteList(u8, u8).Node {
    return @ptrFromInt(p & ~@as(usize, 1));
}

fn test_condition(T: type, C: type) type {
    return struct {
        pub fn match(value: ?*T, expected: C) bool {
            return value != null and value.?.* == expected;
        }
    };
}

test "DestroyBuffer" {
    var destroy_buffer = DestroyBuffer(u8, 2).init(testing.allocator);
    defer {
        testing.allocator.free(destroy_buffer.buffer);
    }

    try testing.expect(destroy_buffer.head == 0);
    try testing.expect(destroy_buffer.buffer[0] == null);

    var t1: u8 = 0;
    const r1 = destroy_buffer.put(&t1);
    try testing.expect(destroy_buffer.head == 1);
    try testing.expect(r1 == null);
    try testing.expect(destroy_buffer.buffer[0].?.* == 0);

    var t2: u8 = 1;
    const r2 = destroy_buffer.put(&t2);
    try testing.expect(destroy_buffer.head == 0);
    try testing.expect(r2 == null);
    try testing.expect(destroy_buffer.buffer[1].?.* == 1);

    var t3: u8 = 2;
    const r3 = destroy_buffer.put(&t3);
    try testing.expect(destroy_buffer.head == 1);
    try testing.expect(r3.?.* == 0);
    try testing.expect(destroy_buffer.buffer[0].?.* == 2);
}

test "Queue#enqueue" {
    var task_queue = Queue(u8, 2).init(testing.allocator);
    defer task_queue.deinit();

    task_queue.enqueue(0);
    try testing.expect(task_queue.tail.entry == 0);
    try testing.expect(task_queue.tail.use_counter == 0);
    try testing.expect(task_queue.head.next.?.entry == 0);

    task_queue.enqueue(1);
    try testing.expect(task_queue.tail.entry == 1);
    try testing.expect(task_queue.tail.use_counter == 0);
    try testing.expect(task_queue.head.next.?.entry == 0);

    task_queue.enqueue(2);
    try testing.expect(task_queue.tail.entry == 2);
    try testing.expect(task_queue.tail.use_counter == 0);
    try testing.expect(task_queue.head.next.?.entry == 0);
}

test "Queue#dequeue" {
    var task_queue = Queue(u8, 2).init(testing.allocator);
    defer task_queue.deinit();

    task_queue.enqueue(0);
    task_queue.enqueue(1);
    task_queue.enqueue(2);
    try testing.expect(task_queue.head.next.?.entry == 0);

    try testing.expect(task_queue.dequeue() == 0);
    try testing.expect(task_queue.head.next.?.entry == 1);

    try testing.expect(task_queue.dequeue() == 1);
    try testing.expect(task_queue.head.next.?.entry == 2);

    try testing.expect(task_queue.dequeue() == 2);
    try testing.expect(task_queue.head.next == null);

    task_queue.enqueue(3);
    try testing.expect(task_queue.tail.entry == 3);
    try testing.expect(task_queue.tail.use_counter == 0);
    try testing.expect(task_queue.head.entry == 2);
    try testing.expect(task_queue.head.use_counter == 0);

    try testing.expect(task_queue.dequeue() == 3);
    try testing.expect(task_queue.tail.use_counter == 0);
    try testing.expect(task_queue.head.use_counter == 0);
    try testing.expect(task_queue.dequeue() == null);
}

test "AppendDeleteList#prepend" {
    var list = try AppendDeleteList(u8, u8).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const head_ptr = to_pointer(list.head).?;

    const v1 = try list.allocator.create(u8);
    v1.* = 0;
    try list.prepend(v1);
    var v1_ptr = to_pointer(head_ptr.next);
    try testing.expect(v1_ptr.?.entry.?.* == 0);

    const v2 = try list.allocator.create(u8);
    v2.* = 1;
    try list.prepend(v2);
    v1_ptr = to_pointer(head_ptr.next);
    const v2_ptr = to_pointer(v1_ptr.?.next);
    try testing.expect(v1_ptr.?.entry.?.* == 1);
    try testing.expect(v2_ptr.?.entry.?.* == 0);
}

test "AppendDeleteList#prepend with marked" {
    var list = try AppendDeleteList(u8, u8).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const head_ptr = to_pointer(list.head).?;

    const v1 = try list.allocator.create(u8);
    v1.* = 0;
    try list.prepend(v1);

    const v2 = try list.allocator.create(u8);
    v2.* = 1;
    try list.prepend(v2);

    list.mark_delete(test_condition(u8, u8).match, 1);

    const v3 = try list.allocator.create(u8);
    v3.* = 2;
    try list.prepend(v3);

    var v1_ptr = to_pointer(head_ptr.next);
    const v2_ptr = to_pointer(v1_ptr.?.next);
    try testing.expect(v1_ptr.?.entry.?.* == 2);
    try testing.expect(v2_ptr.?.entry.?.* == 0);

    list.mark_delete(test_condition(u8, u8).match, 2);
    list.mark_delete(test_condition(u8, u8).match, 0);

    const v4 = try list.allocator.create(u8);
    v4.* = 3;
    try list.prepend(v4);

    v1_ptr = to_pointer(head_ptr.next);
    try testing.expect(v1_ptr.?.entry.?.* == 3);
    try testing.expect(v1_ptr.?.next == 0);
}

test "AppendDeleteList#delete" {
    var list = try AppendDeleteList(u8, u8).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const v1 = try list.allocator.create(u8);
    v1.* = 0;
    try list.prepend(v1);

    const v2 = try list.allocator.create(u8);
    v2.* = 1;
    try list.prepend(v2);

    const v3 = try list.allocator.create(u8);
    v3.* = 2;
    try list.prepend(v3);

    list.mark_delete(test_condition(u8, u8).match, 1);
    var iter = list.iterator();
    while (iter.next()) |n| {
        _ = n;
    }
    try testing.expect(list.delete(test_condition(u8, u8).match, 1));
}

test "AppendDeleteList Iterator" {
    var list = try AppendDeleteList(u8, u8).init(testing.allocator, test_cleanup);
    defer list.deinit();

    const v1 = try list.allocator.create(u8);
    v1.* = 0;
    try list.prepend(v1);

    const v2 = try list.allocator.create(u8);
    v2.* = 1;
    try list.prepend(v2);

    const v3 = try list.allocator.create(u8);
    v3.* = 2;
    try list.prepend(v3);

    var iter = list.iterator();
    var node: ?*AppendDeleteList(u8, u8).Node = iter.next();
    try testing.expect(node.?.entry.?.* == 2);
    node = iter.next();
    try testing.expect(node.?.entry.?.* == 1);
    node = iter.next();
    try testing.expect(node.?.entry.?.* == 0);
    iter.deinit();

    const v1_ptr = to_pointer(to_pointer(list.head).?.next);
    const v2_ptr = to_pointer(v1_ptr.?.next);
    const v3_ptr = to_pointer(v2_ptr.?.next);
    try testing.expect(v1_ptr.?.use_counter == 0);
    try testing.expect(v2_ptr.?.use_counter == 0);
    try testing.expect(v3_ptr.?.use_counter == 0);

    list.mark_delete(test_condition(u8, u8).match, 1);
    iter = list.iterator();
    node = iter.next();
    try testing.expect(node.?.entry.?.* == 2);
    node = iter.next();
    try testing.expect(node.?.entry.?.* == 0);
    node = iter.next();
    try testing.expect(node == null);
}

// fn test_job(list: *AppendDeleteList(u64, u64), thread_number: usize) void {
//     var active_ids = std.ArrayList(u64).init(testing.allocator);
//     defer active_ids.deinit();

//     var marked_delete_ids = std.ArrayList(u64).init(testing.allocator);
//     defer marked_delete_ids.deinit();

//     var operation: u8 = 0;
//     const max_iteration = 625000;
//     for (0..max_iteration) |i| {
//         if ((i % 5000 == 0) or (i > 624500 and i % 100 == 0)) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         if (operation == 0) {
//             const data = list.allocator.create(u64) catch continue;
//             data.* = max_iteration * thread_number + i;
//             active_ids.append(data.*) catch continue;
//             if (i > 624998) {
//                 std.debug.print("{d} [TEST] #prepend\n", .{std.Thread.getCurrentId()});
//                 list.debug_print();
//                 std.debug.print("{d} \n", .{std.Thread.getCurrentId()});
//             }
//             list.prepend(data) catch continue;
//         } else if ((operation > 0 and operation < 6) or (operation >= 7 and operation <= 8)) {
//             var iter = list.iterator();
//             defer iter.deinit();

//             var counter: u8 = 0;
//             while (iter.next()) |_| {
//                 counter += 1;
//                 if (counter >= 10) break;
//             }
//         } else if (operation == 6) {
//             if (active_ids.items.len > 0) {
//                 const data = active_ids.orderedRemove(0);
//                 if (i > 624998) {
//                     std.debug.print("{d} [TEST] #mark_delete\n", .{std.Thread.getCurrentId()});
//                     list.debug_print();
//                     std.debug.print("{d} \n", .{std.Thread.getCurrentId()});
//                 }
//                 list.mark_delete(test_condition(u64, u64).match, data);
//                 marked_delete_ids.append(data) catch continue;
//             }
//         } else {
//             if (marked_delete_ids.items.len > 0) {
//                 const data = marked_delete_ids.orderedRemove(0);
//                 if (i > 624998) {
//                     std.debug.print("{d} [TEST] #delete\n", .{std.Thread.getCurrentId()});
//                     list.debug_print();
//                     std.debug.print("{d} \n", .{std.Thread.getCurrentId()});
//                 }
//                 _ = list.delete(test_condition(u64, u64).match, data);
//             }
//         }
//         operation = (operation + 1) % 10;
//     }
// }

// test "AppendDeleteList concurrecny" {
//     var list = try AppendDeleteList(u64, u64).init(testing.allocator, test_cleanup_u64);

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, test_job, .{ &list, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }

//     std.debug.print("\n", .{});
//     std.debug.print("\n", .{});
//     list.debug_print();
//     std.debug.print("\n", .{});

//     list.deinit();
// }

fn queue_test_job(queue: *Queue(u64, 64), thread_number: usize) void {
    var operation: u8 = 0;
    const max_iteration = 625000;
    for (0..max_iteration) |i| {
        if (i % 5000 == 0) {
            std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
        }

        if (operation == 0) {
            queue.enqueue(thread_number);
        } else {
            _ = queue.dequeue();
        }
        operation = (operation + 1) % 2;
    }
    std.debug.print("[TEST] Thread {d} done\n", .{thread_number});
}

test "Queue concurrecny" {
    var queue = Queue(u64, 64).init(testing.allocator);
    defer queue.deinit();

    var threads: [16]std.Thread = undefined;
    for (0..16) |i| {
        threads[i] = try std.Thread.spawn(.{}, queue_test_job, .{ &queue, i });
    }

    for (threads) |t| {
        t.join();
    }
    std.debug.print("All work is done! Cleaning up...\n", .{});
}

fn destroy_buffer_test_job(buf: *DestroyBuffer(u64, 8), thread_number: usize) void {
    const max_iteration = 625000;
    var data: u64 = thread_number;
    for (0..max_iteration) |i| {
        if (i % 5000 == 0) {
            std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
        }

        var foo = testing.allocator.create(std.ArrayList(u64)) catch unreachable;
        foo.* = std.ArrayList(u64).init(testing.allocator);
        foo.deinit();
        testing.allocator.destroy(foo);
        _ = buf.put(&data);
    }
}

test "DestroyBuffer concurrecny" {
    var buf = DestroyBuffer(u64, 8).init(testing.allocator);

    var threads: [16]std.Thread = undefined;
    for (0..16) |i| {
        threads[i] = try std.Thread.spawn(.{}, destroy_buffer_test_job, .{ &buf, i });
    }

    for (threads) |t| {
        t.join();
    }

    testing.allocator.free(buf.buffer);
}
