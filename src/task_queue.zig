const std = @import("std");

pub const Task = struct {
    context: *anyopaque,
    run_fn: *const fn (ptr: *anyopaque) void,
    destroy_fn: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,

    pub fn run(self: *const Task) void {
        self.run_fn(self.context);
    }

    pub fn destroy(self: *const Task, allocator: std.mem.Allocator) void {
        self.destroy_fn(self.context, allocator);
    }
};

pub const TaskQueue = struct {
    allocator: std.mem.Allocator,
    head: *TaskNode,
    tail: *TaskNode,

    const TaskNode = struct {
        task: Task,
        next: ?*TaskNode,
        prev: ?*TaskNode,
        padding1: u8 align(std.atomic.cache_line) = 0,
    };

    pub fn init(allocator: std.mem.Allocator) TaskQueue {
        var guard_task = NullTask{};

        var prev_guard = allocator.create(TaskNode) catch unreachable;
        prev_guard.* = TaskNode{
            .task = guard_task.task(),
            .next = null,
            .prev = null,
        };

        var start_guard = allocator.create(TaskNode) catch unreachable;
        start_guard.* = TaskNode{
            .task = guard_task.task(),
            .next = null,
            .prev = null,
        };

        start_guard.prev = prev_guard;
        prev_guard.next = start_guard;

        return TaskQueue{
            .allocator = allocator,
            .head = start_guard,
            .tail = start_guard,
        };
    }

    pub fn deinit(self: *TaskQueue) void {
        // TODO: THIS IS NOT THREAD SAFE! Tutn to lock-free
        var head_pointer: ?*TaskNode = self.head;
        if (head_pointer.?.prev) |prev_guard| {
            self.allocator.destroy(prev_guard);
        }
        while (head_pointer != null) {
            const next_node = head_pointer.?.next;
            self.allocator.destroy(head_pointer.?);
            head_pointer = next_node;
        }
    }

    pub fn enqueue(self: *TaskQueue, task: Task) void {
        const node = self.allocator.create(TaskNode) catch unreachable;
        node.* = TaskNode{
            .task = task,
            .next = null,
            .prev = null,
        };

        while (true) {
            var ltail = self.tail;
            var lprev = ltail.prev.?;
            if (lprev.next == null and lprev != ltail) {
                lprev.next = ltail;
            }
            node.prev = ltail;
            if (@cmpxchgWeak(
                *TaskNode,
                &self.tail,
                ltail,
                node,
                .seq_cst,
                .seq_cst,
            ) == null) {
                ltail.next = node;
                return;
            }
        }
    }

    pub fn dequeue(self: *TaskQueue) ?Task {
        while (true) {
            const lhead = self.head;

            const ltail = self.tail;
            var lprev = ltail.prev;
            if (lprev.?.next == null and lprev != ltail) {
                lprev.?.next = ltail;
            }

            const lnext = lhead.next;
            if (lhead == ltail or lnext == null) return null;
            if (lnext.? == lhead) continue;
            if (@cmpxchgWeak(
                *TaskNode,
                &self.head,
                lhead,
                lnext.?,
                .seq_cst,
                .seq_cst,
            ) == null) {
                defer self.allocator.destroy(lhead.prev.?);
                return lnext.?.task;
            }
        }
    }
};

const NullTask = struct {
    pub fn task(self: *NullTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
        };
    }

    fn run(ptr: *anyopaque) void {
        _ = ptr;
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        _ = ptr;
        _ = allocator;
    }
};

// Tests
const testing = std.testing;

const TestTask = struct {
    id: u8,

    fn run(ptr: *anyopaque) void {
        const self: *TestTask = @ptrCast(@alignCast(ptr));
        _ = self.id;
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        // do nothing
        const self: *TestTask = @ptrCast(@alignCast(ptr));
        _ = self.id;
        _ = allocator;
    }

    fn task(self: *TestTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
        };
    }
};

fn visualize_queue(queue: *TaskQueue) void {
    var head_pointer: ?*TaskQueue.TaskNode = queue.head;
    if (head_pointer.?.prev) |prev_guard| {
        std.debug.print("[PREV_GUARD] {*}\n", .{prev_guard});
    }
    std.debug.print("[HEAD] {*}\n", .{queue.head});
    std.debug.print("[TAIL] {*}\n", .{queue.tail});
    while (head_pointer != null) {
        std.debug.print("{*} ", .{head_pointer.?});
        const next_node = head_pointer.?.next;
        head_pointer = next_node;
    }
    std.debug.print("\n\n", .{});
}

test "TaskQueue#enqueue" {
    var task_queue = TaskQueue.init(testing.allocator);
    defer task_queue.deinit();

    var t1 = TestTask{ .id = 0 };
    task_queue.enqueue(t1.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.task.context))) == &t1);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.task.context))) == &t1);

    var t2 = TestTask{ .id = 1 };
    task_queue.enqueue(t2.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.task.context))) == &t2);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.task.context))) == &t1);

    var t3 = TestTask{ .id = 2 };
    task_queue.enqueue(t3.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.task.context))) == &t3);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.task.context))) == &t1);
}

test "TaskQueue#dequeue" {
    var task_queue = TaskQueue.init(testing.allocator);
    defer task_queue.deinit();

    var task: ?Task = undefined;

    var t1 = TestTask{ .id = 0 };
    var t2 = TestTask{ .id = 1 };
    var t3 = TestTask{ .id = 2 };
    task_queue.enqueue(t1.task());
    task_queue.enqueue(t2.task());
    task_queue.enqueue(t3.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.task.context))) == &t1);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t1.id);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.task.context))) == &t2);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t2.id);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.task.context))) == &t3);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t3.id);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.task.context))) == &t3);

    var t4 = TestTask{ .id = 3 };
    task_queue.enqueue(t4.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.task.context))) == &t4);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.task.context))) == &t3);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t4.id);
    task = task_queue.dequeue();
    try testing.expect(task == null);
}

// fn run_task_test(task_queue: *TaskQueue) void {
//     var idle_cycles_counter: u8 = 0;
//     while (true) {
//         const task = task_queue.dequeue();
//         if (task) |t| {
//             defer t.destroy(task_queue.allocator);

//             t.run();
//             std.time.sleep(std.time.ns_per_s / 100);

//             const rand = std.crypto.random;
//             if (rand.boolean() and rand.boolean() and rand.boolean()) {
//                 const new_task = testing.allocator.create(NullTask) catch unreachable;
//                 new_task.* = .{};
//                 task_queue.enqueue(new_task.task());
//             }
//         } else {
//             idle_cycles_counter += 1;
//             if (idle_cycles_counter >= 3) {
//                 return;
//             }
//             std.time.sleep(std.time.ns_per_s / 10);
//         }
//     }
// }

// fn add_task_test(task_queue: *TaskQueue) void {
//     for (0..5000) |i| {
//         if (i % 500 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }
//         const new_task = testing.allocator.create(NullTask) catch unreachable;
//         new_task.* = .{};
//         task_queue.enqueue(new_task.task());
//         std.time.sleep(std.time.ns_per_s / 200);
//     }
// }

// fn check_task_queue(task_queue: *TaskQueue) void {
//     while (task_queue.head != task_queue.tail) {}
//     return;
// }

// test "TaskQueue concurrecny testing" {
//     var task_queue = TaskQueue.init(testing.allocator);

//     var worker_threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         worker_threads[i] = try std.Thread.spawn(.{}, run_task_test, .{&task_queue});
//     }

//     const thread1 = try std.Thread.spawn(.{}, add_task_test, .{&task_queue});
//     const thread2 = try std.Thread.spawn(.{}, add_task_test, .{&task_queue});
//     const thread3 = try std.Thread.spawn(.{}, add_task_test, .{&task_queue});
//     const thread4 = try std.Thread.spawn(.{}, add_task_test, .{&task_queue});

//     const check_thread = try std.Thread.spawn(.{}, check_task_queue, .{&task_queue});

//     thread1.join();
//     thread2.join();
//     thread3.join();
//     thread4.join();
//     for (worker_threads) |wt| {
//         wt.join();
//     }
//     check_thread.join();
//     task_queue.deinit();
// }
