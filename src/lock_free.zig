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
                return null;
            }
        }
    };
}

pub fn AppendOnlyQueue(E: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: *Node,
        tail: *Node,
        node_cleanup_fn: NodeCleanupFn,

        const NodeCleanupFn = *const fn (allocator: std.mem.Allocator, data_ptr: *E) void;
        const Node = struct {
            entry: ?*E,
            next: ?*Node,
            padding1: u8 align(std.atomic.cache_line) = 0,
        };

        pub fn init(allocator: std.mem.Allocator, node_cleanup_fn: NodeCleanupFn) Self {
            const sentinel_node = allocator.create(Node) catch unreachable;
            sentinel_node.* = Node{
                .entry = null,
                .next = null,
            };

            return Self{
                .allocator = allocator,
                .head = sentinel_node,
                .tail = sentinel_node,
                .node_cleanup_fn = node_cleanup_fn,
            };
        }

        pub fn deinit(self: *Self) void {
            var pointer: ?*Node = self.head;
            while (pointer != null) {
                const next_node = pointer.?.next;
                if (pointer.?.entry) |e| {
                    self.node_cleanup_fn(self.allocator, e);
                }
                self.allocator.destroy(pointer.?);
                pointer = next_node;
            }
        }

        pub fn enqueue(self: *Self, entry: *E) void {
            const node = self.allocator.create(Node) catch unreachable;
            node.* = Node{
                .entry = entry,
                .next = null,
            };

            while (true) {
                const ltail = @atomicLoad(*Node, &self.tail, .seq_cst);
                if (@cmpxchgWeak(
                    *Node,
                    &self.tail,
                    ltail,
                    node,
                    .seq_cst,
                    .seq_cst,
                ) == null) {
                    @atomicStore(?*Node, &ltail.next, node, .seq_cst);
                    return;
                }
            }
        }
    };
}

pub fn AppendDeleteList(E: type, C: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: MarkablePointer,
        current_epoch: u8 = 0,
        epoch_counters: [3]u64 = .{ 0, 0, 0 },
        retire_lists: [3]*AppendOnlyQueue(Node),
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
            padding: u8 align(std.atomic.cache_line) = 0,
            node_cleanup_fn: NodeCleanupFn,
        };

        pub fn node_cleanup(allocator: std.mem.Allocator, node: *Node) void {
            if (node.entry) |e| {
                node.node_cleanup_fn(allocator, e);
            }
            allocator.destroy(node);
        }

        const Iterator = struct {
            list: *Self,
            current_epoch: u8,
            previous: *Node,

            pub fn init(list: *Self) Iterator {
                list.try_clean_up_epoch();

                const current_epoch = @atomicLoad(u8, &list.current_epoch, .seq_cst);
                _ = @atomicRmw(u64, &list.epoch_counters[current_epoch], .Add, 1, .seq_cst);

                return .{
                    .list = list,
                    .current_epoch = current_epoch,
                    .previous = to_pure_pointer(list.head).?,
                };
            }

            pub fn deinit(self: *Iterator) void {
                _ = @atomicRmw(u64, &self.list.epoch_counters[self.current_epoch], .Sub, 1, .seq_cst);
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
                            succ = @atomicLoad(MarkablePointer, &curr_ptr.?.next, .seq_cst);
                            if (@cmpxchgWeak(MarkablePointer, &self.previous.next, curr, succ, .seq_cst, .seq_cst) != null) continue :retry;
                            tmp = curr;
                            curr = succ;
                            curr_ptr = to_pure_pointer(curr);

                            self.list.retire_lists[@atomicLoad(u8, &self.list.current_epoch, .seq_cst)].enqueue(to_pure_pointer(tmp).?);
                        }
                        if (curr_ptr == null) return null;

                        self.previous = curr_ptr.?;
                        return curr_ptr.?;
                    }

                    return null;
                }
            }
        };

        pub fn init(allocator: std.mem.Allocator, node_cleanup_fn: NodeCleanupFn) !Self {
            const sentinel_node = try allocator.create(Node);
            sentinel_node.* = Node{
                .entry = null,
                .next = 0,
                .node_cleanup_fn = node_cleanup_fn,
            };

            const aoq0 = try allocator.create(AppendOnlyQueue(Node));
            aoq0.* = AppendOnlyQueue(Node).init(allocator, node_cleanup);
            const aoq1 = try allocator.create(AppendOnlyQueue(Node));
            aoq1.* = AppendOnlyQueue(Node).init(allocator, node_cleanup);
            const aoq2 = try allocator.create(AppendOnlyQueue(Node));
            aoq2.* = AppendOnlyQueue(Node).init(allocator, node_cleanup);

            return .{
                .allocator = allocator,
                .head = to_marked_pointer(sentinel_node, false),
                .retire_lists = .{ aoq0, aoq1, aoq2 },
                .node_cleanup_fn = node_cleanup_fn,
            };
        }

        pub fn deinit(self: *Self) void {
            var pure_pointer: ?*Node = to_pure_pointer(self.head).?;
            while (pure_pointer) |pp| {
                if (pp.entry) |e| {
                    self.node_cleanup_fn(self.allocator, e);
                }
                pure_pointer = to_pure_pointer(pp.next);
                self.allocator.destroy(pp);
            }

            for (self.retire_lists) |retire_list| {
                retire_list.deinit();
                self.allocator.destroy(retire_list);
            }
        }

        pub fn prepend(self: *Self, entry: *E) !void {
            self.try_clean_up_epoch();

            var node = try self.allocator.create(Node);
            node.* = Node{
                .entry = entry,
                .next = 0,
                .node_cleanup_fn = self.node_cleanup_fn,
            };

            const current_epoch = @atomicLoad(u8, &self.current_epoch, .seq_cst);
            _ = @atomicRmw(u64, &self.epoch_counters[current_epoch], .Add, 1, .seq_cst);
            defer _ = @atomicRmw(u64, &self.epoch_counters[current_epoch], .Sub, 1, .seq_cst);

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
                        succ = @atomicLoad(MarkablePointer, &curr_ptr.?.next, .seq_cst);
                        if (@cmpxchgWeak(MarkablePointer, &sent_ptr.next, curr, succ, .seq_cst, .seq_cst) != null) continue :retry;
                        tmp = curr;
                        curr = succ;
                        curr_ptr = to_pure_pointer(curr);

                        self.retire_lists[@atomicLoad(u8, &self.current_epoch, .seq_cst)].enqueue(to_pure_pointer(tmp).?);
                    }
                }

                node.next = curr;
                if (@cmpxchgWeak(MarkablePointer, &sent_ptr.next, node.next, to_marked_pointer(node, false), .seq_cst, .seq_cst) != null) {
                    continue;
                }
                break;
            }
        }

        pub fn mark_delete(self: *Self, condition: Condition, expected: C) void {
            self.try_clean_up_epoch();

            const current_epoch = @atomicLoad(u8, &self.current_epoch, .seq_cst);
            _ = @atomicRmw(u64, &self.epoch_counters[current_epoch], .Add, 1, .seq_cst);
            defer _ = @atomicRmw(u64, &self.epoch_counters[current_epoch], .Sub, 1, .seq_cst);

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
                        succ = @atomicLoad(MarkablePointer, &curr_ptr.?.next, .seq_cst);
                        if (@cmpxchgWeak(MarkablePointer, &prev_ptr.next, curr, succ, .seq_cst, .seq_cst) != null) {
                            continue :retry;
                        }
                        tmp = curr;
                        curr = succ;
                        curr_ptr = to_pure_pointer(curr);

                        self.retire_lists[@atomicLoad(u8, &self.current_epoch, .seq_cst)].enqueue(to_pure_pointer(tmp).?);
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

        pub inline fn iterator(self: *Self) Iterator {
            return Iterator.init(self);
        }

        fn try_clean_up_epoch(self: *Self) void {
            while (true) {
                const current_epoch: u8 = @atomicLoad(u8, &self.current_epoch, .seq_cst);
                const prev_epoch: u8 = if (current_epoch == 0) 2 else (current_epoch - 1);
                var old_aoq = @atomicLoad(*AppendOnlyQueue(Node), &self.retire_lists[prev_epoch], .seq_cst);

                if (self.epoch_counters[prev_epoch] == 0) {
                    const next_epoch: u8 = (current_epoch + 1) % 3;
                    if (@cmpxchgWeak(u8, &self.current_epoch, current_epoch, next_epoch, .seq_cst, .seq_cst) != null) continue;

                    var aoq = self.allocator.create(AppendOnlyQueue(Node)) catch unreachable;
                    aoq.* = AppendOnlyQueue(Node).init(self.allocator, node_cleanup);
                    if (@cmpxchgWeak(*AppendOnlyQueue(Node), &self.retire_lists[prev_epoch], old_aoq, aoq, .seq_cst, .seq_cst) != null) {
                        aoq.deinit();
                        self.allocator.destroy(aoq);
                        continue;
                    }

                    old_aoq.deinit();
                    self.allocator.destroy(old_aoq);
                    return;
                } else {
                    return;
                }
            }
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

test "AppendOnlyQueue" {
    var queue = AppendOnlyQueue(u8).init(testing.allocator, test_cleanup);
    defer queue.deinit();

    const e1 = try testing.allocator.create(u8);
    e1.* = 0;
    queue.enqueue(e1);
    try testing.expect(queue.tail.entry.?.* == 0);
    try testing.expect(queue.head.next.?.entry.?.* == 0);

    const e2 = try testing.allocator.create(u8);
    e2.* = 1;
    queue.enqueue(e2);
    try testing.expect(queue.tail.entry.?.* == 1);
    try testing.expect(queue.head.next.?.entry.?.* == 0);
    try testing.expect(queue.head.next.?.next.?.entry.?.* == 1);

    const e3 = try testing.allocator.create(u8);
    e3.* = 2;
    queue.enqueue(e3);
    try testing.expect(queue.tail.entry.?.* == 2);
    try testing.expect(queue.head.next.?.entry.?.* == 0);
    try testing.expect(queue.head.next.?.next.?.entry.?.* == 1);
    try testing.expect(queue.head.next.?.next.?.next.?.entry.?.* == 2);
}

test "Queue#enqueue" {
    var queue = Queue(u8, 2).init(testing.allocator);
    defer queue.deinit();

    queue.enqueue(0);
    try testing.expect(queue.tail.entry == 0);
    try testing.expect(queue.tail.use_counter == 0);
    try testing.expect(queue.head.next.?.entry == 0);

    queue.enqueue(1);
    try testing.expect(queue.tail.entry == 1);
    try testing.expect(queue.tail.use_counter == 0);
    try testing.expect(queue.head.next.?.entry == 0);

    queue.enqueue(2);
    try testing.expect(queue.tail.entry == 2);
    try testing.expect(queue.tail.use_counter == 0);
    try testing.expect(queue.head.next.?.entry == 0);
}

test "Queue#dequeue" {
    var queue = Queue(u8, 2).init(testing.allocator);
    defer queue.deinit();

    queue.enqueue(0);
    queue.enqueue(1);
    queue.enqueue(2);
    try testing.expect(queue.head.next.?.entry == 0);

    try testing.expect(queue.dequeue() == 0);
    try testing.expect(queue.head.next.?.entry == 1);

    try testing.expect(queue.dequeue() == 1);
    try testing.expect(queue.head.next.?.entry == 2);

    try testing.expect(queue.dequeue() == 2);
    try testing.expect(queue.head.next == null);

    queue.enqueue(3);
    try testing.expect(queue.tail.entry == 3);
    try testing.expect(queue.tail.use_counter == 0);
    try testing.expect(queue.head.entry == 2);
    try testing.expect(queue.head.use_counter == 0);

    try testing.expect(queue.dequeue() == 3);
    try testing.expect(queue.tail.use_counter == 0);
    try testing.expect(queue.head.use_counter == 0);
    try testing.expect(queue.dequeue() == null);
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

//     var operation: u8 = 0;
//     const max_iteration = 625000;
//     for (0..max_iteration) |i| {
//         if (i % 5000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         if (operation == 0) {
//             const data = list.allocator.create(u64) catch continue;
//             data.* = max_iteration * thread_number + i;
//             active_ids.append(data.*) catch continue;
//             list.prepend(data) catch continue;
//         } else if (operation > 0 and operation <= 8) {
//             var iter = list.iterator();
//             defer iter.deinit();

//             var counter: u8 = 0;
//             while (iter.next()) |_| {
//                 counter += 1;
//                 if (counter >= 10) break;
//             }
//         } else {
//             if (active_ids.items.len > 0) {
//                 const data = active_ids.orderedRemove(0);
//                 list.mark_delete(test_condition(u64, u64).match, data);
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

//     list.deinit();
// }

// fn queue_test_job(queue: *Queue(u64, 64), thread_number: usize) void {
//     var operation: u8 = 0;
//     const max_iteration = 625000;
//     for (0..max_iteration) |i| {
//         if (i % 5000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         if (operation == 0) {
//             queue.enqueue(thread_number);
//         } else {
//             _ = queue.dequeue();
//         }
//         operation = (operation + 1) % 2;
//     }
//     std.debug.print("[TEST] Thread {d} done\n", .{thread_number});
// }

// test "Queue concurrecny" {
//     var queue = Queue(u64, 64).init(testing.allocator);
//     defer queue.deinit();

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, queue_test_job, .{ &queue, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
//     std.debug.print("All work is done! Cleaning up...\n", .{});
// }

// fn destroy_buffer_test_job(buf: *DestroyBuffer(u64, 8), thread_number: usize) void {
//     const max_iteration = 625000;
//     var data: u64 = thread_number;
//     for (0..max_iteration) |i| {
//         if (i % 5000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }
//         _ = buf.put(&data);
//     }
// }

// test "DestroyBuffer concurrecny" {
//     var buf = DestroyBuffer(u64, 8).init(testing.allocator);

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, destroy_buffer_test_job, .{ &buf, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }

//     testing.allocator.free(buf.buffer);
// }

// fn append_only_queue_test_job(queue: *AppendOnlyQueue(u64), thread_number: usize) void {
//     const max_iteration = 10000;
//     for (0..max_iteration) |i| {
//         if (i % 100 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         const d = queue.allocator.create(u64) catch unreachable;
//         d.* = thread_number;
//         queue.enqueue(d);
//     }
// }

// test "AppendOnlyQueue concurrecny" {
//     var queue = AppendOnlyQueue(u64).init(testing.allocator, test_cleanup_u64);
//     defer queue.deinit();

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, append_only_queue_test_job, .{ &queue, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
// }
