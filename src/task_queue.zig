const std = @import("std");

pub const Task = struct {
    context: *anyopaque,
    runFn: *const fn (ptr: *anyopaque) void,

    pub fn run(self: *const Task) void {
        self.runFn(self.context);
    }
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

    pub fn enqueue(self: *TaskQueue, task: Task) void {
        const node = self.allocator.create(TaskNode) catch unreachable;
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

    pub fn dequeue(self: *TaskQueue) ?Task {
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

const TestTask = struct {
    id: u8,

    fn run(ptr: *anyopaque) void {
        const self: *TestTask = @ptrCast(@alignCast(ptr));
        _ = self.id;
    }

    fn task(self: *TestTask) Task {
        return .{
            .context = self,
            .runFn = run,
        };
    }
};

test "TaskQueue#enqueue" {
    var task_queue = TaskQueue.init(testing.allocator);
    defer task_queue.deinit();

    try testing.expect(task_queue.head == null);
    try testing.expect(task_queue.tail == null);

    var t1 = TestTask{ .id = 0 };
    task_queue.enqueue(t1.task());
    try testing.expect(task_queue.head != null);
    try testing.expect(task_queue.tail != null);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.?.task.context))) == &t1);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t1);

    var t2 = TestTask{ .id = 1 };
    task_queue.enqueue(t2.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.?.task.context))) == &t2);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t1);

    var t3 = TestTask{ .id = 2 };
    task_queue.enqueue(t3.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.?.task.context))) == &t3);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t1);
}

test "TaskQueue#dequeue" {
    var task_queue = TaskQueue.init(testing.allocator);
    defer task_queue.deinit();

    var t1 = TestTask{ .id = 0 };
    var t2 = TestTask{ .id = 1 };
    var t3 = TestTask{ .id = 2 };
    task_queue.enqueue(t1.task());
    task_queue.enqueue(t2.task());
    task_queue.enqueue(t3.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t1);

    _ = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t2);

    _ = task_queue.dequeue();
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t3);

    _ = task_queue.dequeue();
    try testing.expect(task_queue.head == null);
    try testing.expect(task_queue.tail == null);

    var t4 = TestTask{ .id = 3 };
    task_queue.enqueue(t4.task());
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.tail.?.task.context))) == &t4);
    try testing.expect(@as(*TestTask, @ptrCast(@alignCast(task_queue.head.?.task.context))) == &t4);
}
