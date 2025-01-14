const std = @import("std");
const global_context = @import("./global_context.zig");
const IO = @import("./io.zig").IO;
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

    _ = try std.Thread.spawn(.{}, worker, .{});

    const thread = try std.Thread.spawn(.{}, io_worker, .{});
    thread.join();
}

pub fn io_worker() void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    var io = IO.init(gpa.allocator()) catch {
        std.log.err("Error! Failed to start io worker.\n", .{});
        return;
    };
    defer io.deinit();

    io.listen();
}

pub fn worker() void {
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
