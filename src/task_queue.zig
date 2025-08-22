const std = @import("std");

const global_context = @import("./global_context.zig");
const MetricKind = @import("./metrics.zig").MetricKind;
const Queue = @import("./lock_free.zig").Queue;

pub const Task = struct {
    context: *anyopaque,
    run_fn: *const fn (ptr: *anyopaque) void,
    destroy_fn: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator) void,

    enqued_at: i64 = 0,
    picked_at: i64 = 0,

    pub fn run(self: *Task) void {
        self.picked_at = std.time.microTimestamp();
        _ = global_context.getMetricsAggregator().?.record(.{
            .timestamp = std.time.microTimestamp(),
            .value = @intCast(self.picked_at - self.enqued_at),
            .kind = @intFromEnum(MetricKind.queueWaitTime),
        });

        self.run_fn(self.context);

        const exec_time = std.time.microTimestamp() - self.picked_at;
        _ = global_context.getMetricsAggregator().?.record(.{
            .timestamp = std.time.microTimestamp(),
            .value = @intCast(exec_time),
            .kind = @intFromEnum(MetricKind.taskProcessingTime),
        });
    }

    pub fn destroy(self: *const Task, allocator: std.mem.Allocator) void {
        self.destroy_fn(self.context, allocator);
    }
};

pub const TaskQueue = Queue(Task, 64);

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

fn visualizeQueue(queue: *TaskQueue) void {
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
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.entry.?.context))) == &t1);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.entry.?.context))) == &t1);

    var t2 = TestTask{ .id = 1 };
    task_queue.enqueue(t2.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.entry.?.context))) == &t2);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.entry.?.context))) == &t1);

    var t3 = TestTask{ .id = 2 };
    task_queue.enqueue(t3.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.entry.?.context))) == &t3);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.entry.?.context))) == &t1);
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
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.entry.?.context))) == &t1);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t1.id);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.entry.?.context))) == &t2);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t2.id);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.next.?.entry.?.context))) == &t3);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t3.id);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.entry.?.context))) == &t3);

    var t4 = TestTask{ .id = 3 };
    task_queue.enqueue(t4.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.entry.?.context))) == &t4);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.entry.?.context))) == &t3);

    task = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task.?.context))).id == t4.id);
    task = task_queue.dequeue();
    try testing.expect(task == null);
}

// fn runTaskTest(task_queue: *TaskQueue) void {
//     var idle_cycles_counter: u8 = 0;
//     while (true) {
//         const task = task_queue.dequeue();
//         if (task) |t| {
//             defer t.destroy(task_queue.allocator);

//             t.run();
//             std.time.sleep(std.time.ns_per_s / 100);

//             // const rand = std.crypto.random;
//             // if (rand.boolean() and rand.boolean() and rand.boolean()) {
//             //     const new_task = testing.allocator.create(NullTask) catch unreachable;
//             //     new_task.* = .{};
//             //     task_queue.enqueue(new_task.task());
//             // }
//         } else {
//             idle_cycles_counter += 1;
//             if (idle_cycles_counter >= 3) {
//                 return;
//             }
//             std.time.sleep(std.time.ns_per_s / 10);
//         }
//     }
// }

// fn addTaskTest(task_queue: *TaskQueue) void {
//     for (0..1000) |i| {
//         if (i % 500 == 0) {
//             std.debug.print("[TEST] {d}: {d}\n", .{ std.Thread.getCurrentId(), i });
//         }
//         const new_task = testing.allocator.create(NullTask) catch unreachable;
//         new_task.* = .{};
//         task_queue.enqueue(new_task.task());
//         std.time.sleep(std.time.ns_per_s / 200);
//     }
// }

// fn checkTaskQueue(task_queue: *TaskQueue) void {
//     while (task_queue.head != task_queue.tail) {}
//     return;
// }

// test "TaskQueue concurrecny testing" {
//     var task_queue = TaskQueue.init(testing.allocator);

//     var worker_threads: [4]std.Thread = undefined;
//     for (0..4) |i| {
//         worker_threads[i] = try std.Thread.spawn(.{}, runTaskTest, .{&task_queue});
//     }

//     var threads: [16]std.Thread = undefined;
//     for (0..16) |i| {
//         threads[i] = try std.Thread.spawn(.{}, addTaskTest, .{&task_queue});
//     }

//     const check_thread = try std.Thread.spawn(.{}, checkTaskQueue, .{&task_queue});

//     for (threads) |t| {
//         t.join();
//     }
//     for (worker_threads) |wt| {
//         wt.join();
//     }
//     check_thread.join();
//     task_queue.deinit();
// }
