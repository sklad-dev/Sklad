const std = @import("std");
const global_context = @import("./global_context.zig");

pub fn run_task() void {
    const task_queue = global_context.get_task_queue().?;
    while (true) {
        const task = task_queue.dequeue();
        if (task) |t| {
            t.run();
        } else {
            std.time.sleep(std.time.ns_per_s / 100);
        }
    }
}

// pub const Worker = struct {
//     state: std.atomic.Value(WorkerState) = std.atomic.Value(WorkerState).init(.WAITING),

//     pub const WorkerState = enum(u8) {
//         RUNNING,
//         WAITING,
//         DONE,
//     };

//     pub fn spawn(self: *Worker) void {
//         self.state.store(.RUNNING, .acq_rel);
//         const task_queue = global_context.get_task_queue().?;
//     }
// };

// pub const WorkersPool = struct {
//     allocator: std.mem.Allocator,
//     num_workers: u8,
//     workers: []std.Thread,

//     pub fn run() void {}
// };
