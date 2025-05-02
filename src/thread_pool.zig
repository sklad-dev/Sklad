const std = @import("std");
const global_context = @import("./global_context.zig");
const IoTask = @import("./io.zig").IO.IoTask;

pub fn runTask() void {
    const task_queue = global_context.getTaskQueue().?;
    while (true) {
        const task = task_queue.dequeue();
        if (task) |t| {
            defer t.destroy(task_queue.allocator);

            t.run();
        } else {
            std.time.sleep(std.time.ns_per_s / 10);
        }
    }
}
