const std = @import("std");
const global_context = @import("./global_context.zig");

const MetricsAggregator = @import("./metrics.zig").MetricsAggregator;
const MetricsSnapshot = @import("./metrics.zig").MetricsSnapshot;
const Percentile = @import("./metrics.zig").Percentile;
const MetricKind = @import("./metrics.zig").MetricKind;
const recordMetric = @import("./metrics.zig").recordMetric;

const WorkerContext = struct {
    allocator: std.mem.Allocator,
    block_buffer: []u8,
    reader_buffer: [8]u8 = undefined,
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
    const metrics = global_context.getMetricsAggregator();
    const task_queue = global_context.getTaskQueue().?;
    const configurator = global_context.getConfigurator().?;
    var worker_manager = global_context.getWorkerManager().?;

    initWorkerContext(task_queue.allocator, configurator.sstableBlockSize()) catch |e| {
        std.log.err("Failed to initialize worker context: {any}", .{e});
        return;
    };
    errdefer deinitWorkerContext();

    recordMetric(metrics, MetricKind.workerCounter, 1);
    defer recordMetric(metrics, MetricKind.workerCounter, 0);

    var last_activity = std.time.microTimestamp();
    var idle_iterations: u64 = 0;
    while (true) {
        var task = task_queue.dequeue();
        if (task != null) {
            idle_iterations = 0;
            last_activity = std.time.microTimestamp();
            defer task.?.destroy(task_queue.allocator);
            task.?.run();

            if (shouldSpawnExtraWorker(worker_manager, metrics.?)) {
                _ = worker_manager.trySpawnExtraWorker();
            }
        } else {
            const now = std.time.microTimestamp();
            idle_iterations += 1;

            if (now - last_activity >= worker_manager.idle_timeout_us) {
                if (worker_manager.tryMarkIdleExit()) {
                    break;
                } else {
                    last_activity = now;
                }
            } else {
                if (idle_iterations < 5) {
                    std.atomic.spinLoopHint();
                    continue;
                } else if (idle_iterations < 25) {
                    std.Thread.yield() catch {};
                    continue;
                } else if (idle_iterations < 100) {
                    std.Thread.sleep(100 * std.time.ns_per_us);
                    continue;
                }

                std.Thread.sleep(std.time.ns_per_ms);
            }
        }
    }

    deinitWorkerContext();
}

fn shouldSpawnExtraWorker(manager: *WorkerManager, metrics: *MetricsAggregator) bool {
    var snapshot_buffer: [1]MetricsSnapshot = undefined;
    const num_read = metrics.snapshot_buffer.readUntil(std.time.microTimestamp(), snapshot_buffer[0..]);
    if (num_read == 0) return false;

    const task_wait_p95 = snapshot_buffer[0].queue_wait_percentiles[@intFromEnum(Percentile.p95)];
    return task_wait_p95 >= manager.task_wait_threshold_us and manager.currentWorkerCount() < manager.max_workers;
}

pub const WorkerManager = struct {
    max_workers: u8,
    min_workers: u8,
    num_workers: u8,
    idle_timeout_us: i64,
    task_wait_threshold_us: u64,

    pub fn init(min_workers: u8, max_workers: u8, idle_timeout_us: i64, task_wait_threshold_us: u64) WorkerManager {
        return .{
            .min_workers = min_workers,
            .max_workers = max_workers,
            .num_workers = 0,
            .idle_timeout_us = idle_timeout_us,
            .task_wait_threshold_us = task_wait_threshold_us,
        };
    }

    pub inline fn currentWorkerCount(self: *WorkerManager) u8 {
        return @atomicLoad(u8, &self.num_workers, .acquire);
    }

    pub fn tryMarkIdleExit(self: *WorkerManager) bool {
        var attempts: u8 = 0;
        while (attempts < 5) : (attempts += 1) {
            const current_count = @atomicLoad(u8, &self.num_workers, .acquire);
            if (current_count <= self.min_workers) return false;

            const prev = @cmpxchgStrong(u8, &self.num_workers, current_count, current_count - 1, .acq_rel, .acquire);
            if (prev != null) continue;

            return true;
        }

        return false;
    }

    pub fn trySpawnExtraWorker(self: *WorkerManager) bool {
        var attempts: u8 = 0;
        while (attempts < 5) : (attempts += 1) {
            const current_count = @atomicLoad(u8, &self.num_workers, .acquire);
            if (current_count >= self.max_workers) return false;

            const prev = @cmpxchgStrong(u8, &self.num_workers, current_count, current_count + 1, .acq_rel, .acquire);
            if (prev != null) continue;

            const worker_thread = std.Thread.spawn(.{}, runTask, .{}) catch |e| {
                std.log.err("Failed to spawn extra worker: {any}", .{e});
                _ = @atomicRmw(u8, &self.num_workers, .Sub, 1, .acq_rel);
                return false;
            };
            worker_thread.detach();
            return true;
        }
        return false;
    }
};
