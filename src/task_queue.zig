const std = @import("std");

pub const Task = struct {
    run: u8,
};

pub const TaskQueue = struct {
    allocator: std.mem.Allocator,
    head: ?*TaskNode,
    tail: ?*TaskNode,

    const TaskNode = struct {
        task: Task,
        next: ?*TaskNode,
    };

    pub fn init(allocator: std.mem.Allocator) TaskQueue {
        return TaskQueue{
            .allocator = allocator,
            .head = null,
            .tail = null,
        };
    }

    pub fn deinit(self: *TaskQueue) void {
        // TODO: THIS IS NOT THREAD SAFE! Tutn to lock-free
        if (self.head) |head| {
            var head_pointer: ?*TaskNode = head;
            while (head_pointer != null) {
                const next_node = head_pointer.?.next;
                self.allocator.destroy(head_pointer.?);
                head_pointer = next_node;
            }
        }
    }

    pub fn enqueue(self: *TaskQueue, task: Task) !void {
        const node = try self.allocator.create(TaskNode);
        node.* = TaskNode{
            .task = task,
            .next = null,
        };
        var old_tail = @atomicLoad(?*TaskNode, &self.tail, .seq_cst);
        if (old_tail != null) {
            while (@cmpxchgWeak(
                ?*TaskNode,
                &old_tail.?.next,
                null,
                node,
                .seq_cst,
                .seq_cst,
            ) != null) {
                old_tail = @atomicLoad(?*TaskNode, &self.tail, .seq_cst);
            }
        } else {
            _ = @cmpxchgWeak(?*TaskNode, &self.head, null, node, .seq_cst, .seq_cst);
        }
        _ = @cmpxchgWeak(?*TaskNode, &self.tail, old_tail, node, .seq_cst, .seq_cst);
    }

    pub fn dequeue(self: *TaskQueue) !?Task {
        var old_head = @atomicLoad(?*TaskNode, &self.head, .seq_cst);
        while (old_head != null and @cmpxchgWeak(
            ?*TaskNode,
            &self.head,
            old_head,
            old_head.?.next,
            .seq_cst,
            .seq_cst,
        ) != null) {
            old_head = @atomicLoad(?*TaskNode, &self.head, .seq_cst);
        }
        _ = @cmpxchgWeak(?*TaskNode, &self.tail, old_head, null, .seq_cst, .seq_cst);
        if (old_head != null) {
            const task = old_head.?.task;
            defer self.allocator.destroy(old_head.?);
            return task;
        } else {
            return null;
        }
    }
};

// Tests
const testing = std.testing;

test "TaskQueue#enqueue" {
    var task_queue = TaskQueue.init(testing.allocator);
    defer task_queue.deinit();

    try testing.expect(task_queue.head == null);
    try testing.expect(task_queue.tail == null);

    try task_queue.enqueue(Task{ .run = 0 });
    try testing.expect(task_queue.head != null);
    try testing.expect(task_queue.tail != null);
    try testing.expect(task_queue.tail.?.task.run == 0);
    try testing.expect(task_queue.head.?.task.run == 0);

    try task_queue.enqueue(Task{ .run = 1 });
    try testing.expect(task_queue.tail.?.task.run == 1);
    try testing.expect(task_queue.head.?.task.run == 0);

    try task_queue.enqueue(Task{ .run = 2 });
    try testing.expect(task_queue.tail.?.task.run == 2);
    try testing.expect(task_queue.head.?.task.run == 0);
}

test "TaskQueue#dequeue" {
    var task_queue = TaskQueue.init(testing.allocator);
    defer task_queue.deinit();

    try task_queue.enqueue(Task{ .run = 0 });
    try task_queue.enqueue(Task{ .run = 1 });
    try task_queue.enqueue(Task{ .run = 2 });
    try testing.expect(task_queue.head.?.task.run == 0);

    _ = try task_queue.dequeue();
    try testing.expect(task_queue.head.?.task.run == 1);

    _ = try task_queue.dequeue();
    try testing.expect(task_queue.head.?.task.run == 2);

    _ = try task_queue.dequeue();
    try testing.expect(task_queue.head == null);
    try testing.expect(task_queue.tail == null);

    try task_queue.enqueue(Task{ .run = 3 });
    try testing.expect(task_queue.tail.?.task.run == 3);
    try testing.expect(task_queue.head.?.task.run == 3);
}
