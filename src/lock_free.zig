const std = @import("std");

pub fn RefCounted(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: ?std.mem.Allocator,
        ref_count: std.atomic.Value(u64),
        value: T,

        pub fn init(allocator: ?std.mem.Allocator, value: T) Self {
            return .{
                .allocator = allocator,
                .ref_count = std.atomic.Value(u64).init(1),
                .value = value,
            };
        }

        pub fn release(self: *Self) u64 {
            const prev_count = self.ref_count.fetchSub(1, .acq_rel);
            if (prev_count == 1) {
                if (comptime blk: {
                    const info = @typeInfo(T);
                    break :blk switch (info) {
                        .@"struct" => @hasDecl(T, "deinit"),
                        else => false,
                    };
                }) {
                    self.value.deinit();
                }
                if (self.allocator) |alloc| {
                    alloc.destroy(self);
                }
                return 0;
            }
            return prev_count - 1;
        }

        pub inline fn acquire(self: *Self) u64 {
            return self.ref_count.fetchAdd(1, .acq_rel) + 1;
        }

        pub inline fn tryAcquire(self: *Self) ?u64 {
            var attempts: u64 = 0;
            var current = self.ref_count.load(.acquire);
            while (current > 0) : (attempts += 1) {
                if (self.ref_count.cmpxchgStrong(current, current + 1, .acq_rel, .acquire)) |updated| {
                    current = updated;
                } else {
                    return current + 1;
                }
            }
            return null;
        }

        pub inline fn get(self: *Self) *T {
            return &self.value;
        }

        pub inline fn getConst(self: *const Self) *const T {
            return &self.value;
        }
    };
}

// S have to be power of 2 so it is possible to use bitwise and to compute modulo
pub fn DestroyBuffer(E: type, comptime S: usize) type {
    comptime {
        if (S == 0 or (S & (S - 1)) != 0) {
            @compileError("DestroyBuffer: S must be a non-zero power of two");
        }
    }

    return struct {
        head: u64,
        buffer: []?*E,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) Self {
            const buffer = allocator.alloc(?*E, S) catch unreachable;
            @memset(buffer, null);

            return .{
                .head = 0,
                .buffer = buffer,
            };
        }

        pub inline fn put(self: *Self, entry: *E) ?*E {
            const old_index = @atomicRmw(u64, &self.head, .Add, 1, .acq_rel);
            const slot = @as(usize, @intCast(old_index & (S - 1)));
            const to_delete = @atomicRmw(?*E, &self.buffer[slot], .Xchg, entry, .acq_rel);
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
            use_counter: u32 align(std.atomic.cache_line),
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
                                } else {
                                    std.atomic.spinLoopHint();
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

pub fn AppendOnlyQueue(E: type, nodeCleanupFn: ?*const fn (allocator: std.mem.Allocator, data: E) void) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: *Node,
        tail: *Node,

        const Node = struct {
            entry: ?E,
            next: ?*Node align(std.atomic.cache_line),
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            const sentinel_node = allocator.create(Node) catch unreachable;
            sentinel_node.* = Node{
                .entry = null,
                .next = null,
            };

            return Self{
                .allocator = allocator,
                .head = sentinel_node,
                .tail = sentinel_node,
            };
        }

        pub fn deinit(self: *Self) void {
            var pointer: ?*Node = self.head;
            while (pointer != null) {
                const next_node = pointer.?.next;
                if (pointer.?.entry) |e| {
                    if (comptime nodeCleanupFn) |fn_ptr| {
                        fn_ptr(self.allocator, e);
                    }
                }
                self.allocator.destroy(pointer.?);
                pointer = next_node;
            }
        }

        pub fn enqueue(self: *Self, entry: E) void {
            const node = self.allocator.create(Node) catch unreachable;
            node.* = Node{ .entry = entry, .next = null };
            const prev = @atomicRmw(*Node, &self.tail, .Xchg, node, .acq_rel);
            @atomicStore(?*Node, &prev.next, node, .release);
        }
    };
}

pub fn AddOnlyStack(E: type, nodeCleanupFn: ?*const fn (allocator: std.mem.Allocator, data: E) void) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        head: ?*Node,

        const Node = struct {
            entry: E,
            next: ?*Node align(std.atomic.cache_line),
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .allocator = allocator,
                .head = null,
            };
        }

        pub fn deinit(self: *Self) void {
            var pointer: ?*Node = @atomicLoad(?*Node, &self.head, .acquire);
            while (pointer) |node| {
                const next_node = node.next;
                if (comptime nodeCleanupFn) |fn_ptr| {
                    fn_ptr(self.allocator, node.entry);
                }
                self.allocator.destroy(node);
                pointer = next_node;
            }
        }

        pub fn push(self: *Self, entry: E) void {
            const node = self.allocator.create(Node) catch unreachable;
            node.* = Node{ .entry = entry, .next = undefined };

            while (true) {
                const current_head = @atomicLoad(?*Node, &self.head, .acquire);
                node.next = current_head;

                if (@cmpxchgWeak(
                    ?*Node,
                    &self.head,
                    current_head,
                    node,
                    .acq_rel,
                    .acquire,
                ) == null) {
                    break;
                }
            }
        }
    };
}

pub fn BoundedQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        const Slot = struct {
            seq: usize,
            value: T,
        };

        allocator: std.mem.Allocator,
        buf: []Slot,
        mask: usize, // capacity - 1
        head: usize align(std.atomic.cache_line),
        tail: usize align(std.atomic.cache_line),

        // capacity must be a power of two
        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            if (capacity == 0 or (capacity & (capacity - 1)) != 0) {
                return error.CapacityMustBePowerOfTwo;
            }

            const buf = try allocator.alloc(Slot, capacity);

            var i: usize = 0;
            while (i < capacity) : (i += 1) {
                buf[i].seq = i;
            }

            return .{
                .allocator = allocator,
                .buf = buf,
                .mask = capacity - 1,
                .head = 0,
                .tail = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buf);
        }

        pub fn enqueue(self: *Self, value: T) bool {
            while (true) {
                const pos = @atomicLoad(usize, &self.tail, .monotonic);
                const slot = &self.buf[pos & self.mask];

                const seq = @atomicLoad(usize, &slot.seq, .acquire);
                const diff = @as(isize, @intCast(seq)) - @as(isize, @intCast(pos));

                if (diff == 0) {
                    if (@cmpxchgStrong(usize, &self.tail, pos, pos + 1, .acq_rel, .monotonic) == null) {
                        slot.value = value;
                        @atomicStore(usize, &slot.seq, pos + 1, .release);
                        return true;
                    }
                    continue;
                } else if (diff < 0) {
                    return false;
                } else {
                    std.atomic.spinLoopHint();
                }
            }
        }

        pub fn dequeue(self: *Self) ?T {
            while (true) {
                const pos = @atomicLoad(usize, &self.head, .acquire);
                const slot = &self.buf[pos & self.mask];

                const seq = @atomicLoad(usize, &slot.seq, .acquire);
                const expected = pos + 1;

                const diff = @as(isize, @intCast(seq)) - @as(isize, @intCast(expected));
                if (diff == 0) {
                    if (@cmpxchgWeak(usize, &self.head, pos, pos + 1, .acq_rel, .acquire)) |_| {
                        continue;
                    }
                    const out = slot.value;
                    @atomicStore(usize, &slot.seq, pos + 1 + self.mask, .release);
                    return out;
                } else if (diff < 0) {
                    return null;
                }

                std.atomic.spinLoopHint();
            }
        }
    };
}

// SPMC ring buffer
pub fn RingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        const Slot = struct {
            seq: usize,
            value: T,
        };

        allocator: std.mem.Allocator,
        buf: []Slot,
        mask: usize, // capacity - 1
        head: usize,

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            if (capacity == 0 or (capacity & (capacity - 1)) != 0) {
                return error.CapacityMustBePowerOfTwo;
            }

            const buf = try allocator.alloc(Slot, capacity);

            var i: usize = 0;
            while (i < capacity) : (i += 1) {
                buf[i].seq = i;
            }

            return .{
                .allocator = allocator,
                .buf = buf,
                .mask = capacity - 1,
                .head = 0,
            };
        }

        pub inline fn deinit(self: *Self) void {
            self.allocator.free(self.buf);
        }

        pub fn push(self: *Self, value: *const T) void {
            const pos = @atomicLoad(usize, &self.head, .monotonic);
            const index = pos & self.mask;
            self.buf[index].value = value.*;
            @atomicStore(usize, &self.buf[index].seq, pos + 1, .release);
            _ = @atomicRmw(usize, &self.head, .Add, 1, .monotonic);
        }

        pub fn readLatestOffset(self: *const Self, offset: usize) ?T {
            const head = @atomicLoad(usize, &self.head, .acquire);
            if (head == 0 or offset >= self.buf.len or offset >= head) return null;

            const prev = head - offset - 1;
            const index = prev & self.mask;
            return self.readAt(prev, index);
        }

        fn readAt(self: *const Self, position: u64, index: usize) ?T {
            const expected = position + 1;
            const seq_pre = @atomicLoad(usize, &self.buf[index].seq, .acquire);

            if (seq_pre != expected) return null;

            const seq_post = @atomicLoad(usize, &self.buf[index].seq, .acquire);
            if (seq_pre == seq_post) {
                return self.buf[index].value;
            }

            return null;
        }
    };
}

pub const Arena = struct {
    allocator: std.mem.Allocator,
    arena: []u8,
    current_offset: u64 = 0,

    const ALIGNMENT: u64 = @alignOf(usize);
    const assert = std.debug.assert;

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
        assert(data_size > 0);
        assert(data_size < self.arena.len);

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

        assert(current_offset + data_size <= self.arena.len);
        return current_offset;
    }

    pub inline fn currentOffset(self: *const Arena) u64 {
        return @atomicLoad(u64, &self.current_offset, .seq_cst);
    }

    pub inline fn reset(self: *Arena) void {
        @atomicStore(u64, &self.current_offset, 0, .release);
    }

    pub fn deinit(self: *Arena) void {
        self.allocator.free(self.arena);
    }
};

// Testing
const testing = std.testing;

fn testCleanupRefU8(allocator: std.mem.Allocator, data: *u8) void {
    allocator.destroy(data);
}

fn testCleanupRefU64(allocator: std.mem.Allocator, data: *u64) void {
    allocator.destroy(data);
}

fn testCondition(T: type, C: type) type {
    return struct {
        pub fn match(value: ?*T, expected: C) bool {
            return value != null and value.?.* == expected;
        }
    };
}

test "RefCounted no allocator, no deinit" {
    const RefCountedU64 = RefCounted(u64);
    var rc = RefCountedU64.init(null, 42);

    try testing.expect(rc.get().* == 42);
    try testing.expect(rc.acquire() == 2);
    try testing.expect(rc.release() == 1);
    try testing.expect(rc.release() == 0);
}

const TestStruct = struct {
    allocator: std.mem.Allocator,
    value: *u64,

    pub fn init(allocator: std.mem.Allocator, value: u64) TestStruct {
        const v = allocator.create(u64) catch unreachable;
        v.* = value;

        return .{
            .allocator = allocator,
            .value = v,
        };
    }

    pub fn deinit(self: *TestStruct) void {
        self.allocator.destroy(self.value);
    }
};

test "RefCounted no allocator, deinit" {
    const RefCountedU64 = RefCounted(TestStruct);
    var rc = RefCountedU64.init(
        null,
        TestStruct.init(testing.allocator, 42),
    );

    try testing.expect(rc.get().value.* == 42);
    try testing.expect(rc.acquire() == 2);
    try testing.expect(rc.release() == 1);
    try testing.expect(rc.release() == 0);
}

test "RefCounted allocator, deinit" {
    const RefCountedU64 = RefCounted(TestStruct);
    var rc = try testing.allocator.create(RefCountedU64);
    rc.* = RefCountedU64.init(
        testing.allocator,
        TestStruct.init(testing.allocator, 42),
    );

    try testing.expect(rc.get().value.* == 42);
    try testing.expect(rc.acquire() == 2);
    try testing.expect(rc.release() == 1);
    try testing.expect(rc.release() == 0);
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
    try testing.expect(destroy_buffer.head == 2);
    try testing.expect(r2 == null);
    try testing.expect(destroy_buffer.buffer[1].?.* == 1);

    var t3: u8 = 2;
    const r3 = destroy_buffer.put(&t3);
    try testing.expect(destroy_buffer.head == 3);
    try testing.expect(r3.?.* == 0);
    try testing.expect(destroy_buffer.buffer[0].?.* == 2);
}

test "AppendOnlyQueue, reference type with cleanup" {
    var queue = AppendOnlyQueue(*u8, testCleanupRefU8).init(testing.allocator);
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

test "AppendOnlyQueue, value type, no cleanup" {
    var queue = AppendOnlyQueue(u8, null).init(testing.allocator);
    defer queue.deinit();

    queue.enqueue(0);
    try testing.expect(queue.tail.entry.? == 0);
    try testing.expect(queue.head.next.?.entry.? == 0);

    queue.enqueue(1);
    try testing.expect(queue.tail.entry.? == 1);
    try testing.expect(queue.head.next.?.entry.? == 0);
    try testing.expect(queue.head.next.?.next.?.entry.? == 1);

    queue.enqueue(2);
    try testing.expect(queue.tail.entry.? == 2);
    try testing.expect(queue.head.next.?.entry.? == 0);
    try testing.expect(queue.head.next.?.next.?.entry.? == 1);
    try testing.expect(queue.head.next.?.next.?.next.?.entry.? == 2);
}

test "AddOnlyStack, reference type with cleanup" {
    var stack = AddOnlyStack(*u8, testCleanupRefU8).init(testing.allocator);
    defer stack.deinit();

    const e1 = try testing.allocator.create(u8);
    e1.* = 0;
    stack.push(e1);
    try testing.expect(stack.head.?.entry.* == 0);
    try testing.expect(stack.head.?.next == null);

    const e2 = try testing.allocator.create(u8);
    e2.* = 1;
    stack.push(e2);
    try testing.expect(stack.head.?.entry.* == 1);
    try testing.expect(stack.head.?.next.?.entry.* == 0);

    const e3 = try testing.allocator.create(u8);
    e3.* = 2;
    stack.push(e3);
    try testing.expect(stack.head.?.entry.* == 2);
    try testing.expect(stack.head.?.next.?.entry.* == 1);
    try testing.expect(stack.head.?.next.?.next.?.entry.* == 0);
    try testing.expect(stack.head.?.next.?.next.?.next == null);
}

test "AddOnlyStack, value type, no cleanup" {
    var stack = AddOnlyStack(u8, null).init(testing.allocator);
    defer stack.deinit();

    stack.push(0);
    try testing.expect(stack.head.?.entry == 0);
    try testing.expect(stack.head.?.next == null);

    stack.push(1);
    try testing.expect(stack.head.?.entry == 1);
    try testing.expect(stack.head.?.next.?.entry == 0);

    stack.push(2);
    try testing.expect(stack.head.?.entry == 2);
    try testing.expect(stack.head.?.next.?.entry == 1);
    try testing.expect(stack.head.?.next.?.next.?.entry == 0);
    try testing.expect(stack.head.?.next.?.next.?.next == null);
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

test "BoundedQueue#enqueue" {
    var q = try BoundedQueue(u64).init(testing.allocator, 4);
    defer q.deinit();

    var result: bool = undefined;
    for (0..4) |i| {
        result = q.enqueue(i);
        try testing.expect(result == true);
        try testing.expect(q.buf[i].value == i);
    }

    result = q.enqueue(4);
    try testing.expect(result == false);
}

test "BoundedQueue#dequeue" {
    var q = try BoundedQueue(u64).init(testing.allocator, 4);
    defer q.deinit();

    for (0..4) |i| {
        _ = q.enqueue(i);
    }

    var result: ?u64 = undefined;
    for (0..4) |i| {
        result = q.dequeue();
        try testing.expect(result == i);
    }

    result = q.dequeue();
    try testing.expect(result == null);
}

test "BoundedQueue circular" {
    var q = try BoundedQueue(u64).init(testing.allocator, 4);
    defer q.deinit();

    for (0..4) |i| {
        _ = q.enqueue(i);
    }

    for (0..3) |_| {
        _ = q.dequeue();
    }

    try testing.expect(q.enqueue(4) == true);
    try testing.expect(q.enqueue(5) == true);
    try testing.expect(q.enqueue(6) == true);
    try testing.expect(q.dequeue() == 3);
    try testing.expect(q.enqueue(7) == true);
}

test "RingBuffer" {
    var ring_buffer = try RingBuffer(usize).init(testing.allocator, 4);
    defer ring_buffer.deinit();

    var value: usize = 0;

    ring_buffer.push(&value);
    try testing.expect(ring_buffer.buf[0].value == 0);
    try testing.expect(ring_buffer.readLatestOffset(0).? == 0);
    try testing.expect(ring_buffer.readLatestOffset(1) == null);

    // first cycle
    for (1..4) |i| {
        value += 1;
        ring_buffer.push(&value);
        try testing.expect(ring_buffer.buf[i].value == i);
        try testing.expect(ring_buffer.readLatestOffset(i + 1) == null);
    }
    try testing.expect(ring_buffer.head == 4);
    try testing.expect(ring_buffer.readLatestOffset(ring_buffer.buf.len) == null);
    try testing.expect(ring_buffer.readLatestOffset(ring_buffer.buf.len + 1) == null);

    for (0..4) |i| {
        try testing.expect(ring_buffer.readLatestOffset(i).? == 3 - i);
    }

    // second cycle
    for (0..4) |i| {
        value += 1;
        ring_buffer.push(&value);
        try testing.expect(ring_buffer.buf[i].value == i + ring_buffer.buf.len);
    }
    try testing.expect(ring_buffer.head == 8);

    for (0..4) |i| {
        try testing.expect(ring_buffer.readLatestOffset(i).? == ring_buffer.buf.len * 2 - 1 - i);
    }
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

// fn queueTestJob(queue: *Queue(u64, 64), thread_number: usize) void {
//     var operation: u8 = 0;
//     const max_iteration = 50000;
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

// test "Queue concurrency" {
//     var queue = Queue(u64, 64).init(testing.allocator);
//     defer queue.deinit();

//     var threads: [32]std.Thread = undefined;
//     for (0..32) |i| {
//         threads[i] = try std.Thread.spawn(.{}, queueTestJob, .{ &queue, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
//     std.debug.print("All work is done! Cleaning up...\n", .{});
// }

// fn destroyBufferTestJob(buf: *DestroyBuffer(u64, 8), thread_number: usize) void {
//     const max_iteration = 625000;
//     var data: u64 = thread_number;
//     for (0..max_iteration) |i| {
//         if (i % 5000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }
//         _ = buf.put(&data);
//     }
// }

// test "DestroyBuffer concurrency" {
//     var buf = DestroyBuffer(u64, 8).init(testing.allocator);

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, destroyBufferTestJob, .{ &buf, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }

//     testing.allocator.free(buf.buffer);
// }

// fn appendOnlyQueueTestJob(queue: *AppendOnlyQueue(*u64, testCleanupRefU64), thread_number: usize) void {
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

// test "AppendOnlyQueue concurrency" {
//     var queue = AppendOnlyQueue(*u64, testCleanupRefU64).init(testing.allocator);
//     defer queue.deinit();

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, appendOnlyQueueTestJob, .{ &queue, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
// }

// fn addOnlyStackTestJob(stack: *AddOnlyStack(*u64, testCleanupRefU64), thread_number: usize) void {
//     const max_iteration = 10000;
//     for (0..max_iteration) |i| {
//         if (i % 100 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         const d = stack.allocator.create(u64) catch unreachable;
//         d.* = thread_number;
//         stack.push(d);
//     }
// }

// test "AddOnlyStack concurrency" {
//     var stack = AddOnlyStack(*u64, testCleanupRefU64).init(testing.allocator);
//     defer stack.deinit();

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, addOnlyStackTestJob, .{ &stack, i });
//     }

//     for (threads) |t| {
//         t.join();
//     }
// }

// var should_exit = std.atomic.Value(bool).init(false);

// fn boundedQueueEnqueueTestJob(queue: *BoundedQueue(u64), thread_number: usize) void {
//     const max_iteration = 625000;
//     const rand = std.crypto.random;

//     for (0..max_iteration) |i| {
//         if (i % 1000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         _ = queue.enqueue(thread_number);
//         std.Thread.sleep(rand.intRangeAtMost(u64, 5, 20));
//     }
// }

// fn boundedQueueDequeueTestJob(queue: *BoundedQueue(u64)) void {
//     while (!should_exit.load(.monotonic)) {
//         _ = queue.dequeue();
//     }
// }

// test "BoundedQueue concurrency" {
//     var queue = try BoundedQueue(u64).init(testing.allocator, 4096);
//     defer queue.deinit();

//     var enqueue_threads: [8]std.Thread = undefined;
//     for (0..8) |i| {
//         enqueue_threads[i] = try std.Thread.spawn(.{}, boundedQueueEnqueueTestJob, .{ &queue, i });
//     }

//     _ = try std.Thread.spawn(.{}, boundedQueueDequeueTestJob, .{&queue});

//     for (enqueue_threads) |t| {
//         t.join();
//     }
//     should_exit.store(true, .monotonic);
// }

// var should_exit = std.atomic.Value(bool).init(false);

// fn ringBufferPushTestJob(buffer: *RingBuffer(u64), thread_number: usize) void {
//     const max_iteration = 625000;
//     const rand = std.crypto.random;

//     for (0..max_iteration) |i| {
//         if (i % 1000 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }

//         buffer.push(&thread_number);
//         std.Thread.sleep(rand.intRangeAtMost(u64, 5, 20));
//     }
// }

// fn ringBufferReadTestJob(buffer: *RingBuffer(u64)) void {
//     const rand = std.crypto.random;
//     var head: usize = undefined;
//     while (!should_exit.load(.monotonic)) {
//         head = @atomicLoad(usize, &buffer.head, .acquire);
//         _ = buffer.readLatestOffset(rand.intRangeAtMost(u64, 0, (if (head < buffer.mask) head else buffer.mask)));
//     }
// }

// test "RingBuffer concurrency" {
//     var buffer = try RingBuffer(u64).init(testing.allocator, 4096);
//     defer buffer.deinit();

//     const write_thread = try std.Thread.spawn(.{}, ringBufferPushTestJob, .{ &buffer, 0 });

//     var read_threads: [8]std.Thread = undefined;
//     for (0..8) |i| {
//         read_threads[i] = try std.Thread.spawn(.{}, ringBufferReadTestJob, .{&buffer});
//     }

//     write_thread.join();
//     should_exit.store(true, .monotonic);
//     for (read_threads) |t| {
//         t.join();
//     }
// }
