const std = @import("std");
const global_context = @import("./global_context.zig");

const WorkerContext = struct {
    allocator: std.mem.Allocator,
    block_buffer: []u8,
    reader_buffer: [4]u8 = undefined,
};

threadlocal var WORKER_CONTEXT: ?*WorkerContext = null;

pub fn initWorkerContext(allocator: std.mem.Allocator, block_size: u32) !void {
    if (WORKER_CONTEXT != null) return;

    const ctx = try allocator.create(WorkerContext);
    ctx.* = .{
        .allocator = allocator,
        .block_buffer = try allocator.alloc(u8, block_size),
    };
    WORKER_CONTEXT = ctx;
}

pub fn deinitWorkerContext() void {
    if (WORKER_CONTEXT) |ctx| {
        ctx.allocator.free(ctx.block_buffer);
        ctx.allocator.destroy(ctx);
        WORKER_CONTEXT = null;
    }
}

pub fn getWorkerContext() ?*WorkerContext {
    return WORKER_CONTEXT;
}

pub fn runTask() void {
    const task_queue = global_context.getTaskQueue().?;

    const configurator = global_context.getConfigurator().?;
    initWorkerContext(task_queue.allocator, configurator.sstableBlockSize()) catch |e| {
        std.log.err("Failed to initialize worker context: {any}", .{e});
        return;
    };
    errdefer deinitWorkerContext();

    while (true) {
        var task = task_queue.dequeue();
        if (task != null) {
            defer task.?.destroy(task_queue.allocator);
            task.?.run();
        } else {
            std.Thread.sleep(std.time.ns_per_ms);
        }
    }

    deinitWorkerContext();
}
