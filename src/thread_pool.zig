const std = @import("std");
const global_context = @import("./global_context.zig");
const IoTask = @import("./io.zig").IO.IoTask;

pub fn runTask() void {
    const task_queue = global_context.getTaskQueue().?;
    while (true) {
        var task = task_queue.dequeue();
        if (task != null) {
            defer task.?.destroy(task_queue.allocator);

            task.?.run();
        } else {
            std.time.sleep(std.time.ns_per_ms);
        }
    }
}
