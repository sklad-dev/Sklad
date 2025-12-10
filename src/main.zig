const std = @import("std");

const global_context = @import("./global_context.zig");
const worker = @import("./worker.zig");
const io = @import("./io.zig");

const JsonConfigurator = @import("./json_configurator.zig").JsonConfigurator;
const TypedStorage = @import("./typed_storage.zig").TypedStorage;
const TaskQueue = @import("./task_queue.zig").TaskQueue;
const MetricsAggregator = @import("./metrics.zig").MetricsAggregator;
const WorkerManager = @import("./worker.zig").WorkerManager;
const runMetricAggregator = @import("./metrics.zig").runMetricAggregator;

const DEFAULT_CONFIGURATION_FILE_PATH = @import("./json_configurator.zig").DEFAULT_CONFIGURATION_FILE_PATH;

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    var json_conf = try JsonConfigurator.init(allocator, DEFAULT_CONFIGURATION_FILE_PATH);

    var conf = json_conf.configurator();
    global_context.loadConfiguration(&conf);
    std.log.info("Configuration is loaded", .{});

    worker.initWorkerContext(allocator, conf.sstableBlockSize()) catch |e| {
        std.log.err("Failed to initialize worker context: {any}", .{e});
        return;
    };
    var storage = try TypedStorage.init(allocator);
    defer storage.stop();
    std.log.info("Storage engine is initialized", .{});

    worker.deinitWorkerContext();

    var task_queue = TaskQueue.init(allocator);
    std.log.info("Task queue is initialized", .{});

    var metrics = try MetricsAggregator.init(allocator, 4096, 64);
    defer metrics.stop();
    std.log.info("Metrics aggregator is initialized", .{});

    var worker_manager = WorkerManager.init(
        conf.minWorkers(),
        conf.maxWorkers(),
        conf.idleTimeout() * std.time.us_per_s,
        conf.taskWaitThreshold(),
    );

    global_context.init(&storage, &task_queue, &metrics, &worker_manager);

    var metrics_thread = try std.Thread.spawn(.{}, runMetricAggregator, .{});
    metrics_thread.detach();

    for (0..conf.minWorkers()) |_| {
        _ = worker_manager.trySpawnExtraWorker();
    }

    const thread = try std.Thread.spawn(.{}, io.runIoWorker, .{});
    std.log.info("Listening port {d}", .{io.DEFAULT_PORT});
    thread.join();

    task_queue.deinit();
}
