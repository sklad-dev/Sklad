const std = @import("std");
const global_context = @import("./global_context.zig");
const thread_pool = @import("./thread_pool.zig");
const io = @import("./io.zig");
const GraphStorage = @import("./graph_storage.zig").GraphStorage;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    var graph_storage = try GraphStorage.init(gpa.allocator(), 256, 256);
    defer graph_storage.stop();

    var task_queue = TaskQueue.init(gpa.allocator());
    defer task_queue.deinit();

    global_context.init(&graph_storage, &task_queue);

    var worker_thread = try std.Thread.spawn(.{}, thread_pool.run_task, .{});
    worker_thread.detach();

    const thread = try std.Thread.spawn(.{}, io.run_io_worker, .{});
    thread.join();
}
