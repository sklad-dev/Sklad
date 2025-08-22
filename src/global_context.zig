const std = @import("std");

const Configurator = @import("./configurator.zig").Configurator;
const TypedStorage = @import("./typed_storage.zig").TypedStorage;
const TaskQueue = @import("./task_queue.zig").TaskQueue;
const MetricsAggregator = @import("./metrics.zig").MetricsAggregator;

var CONFIGURATOR = std.atomic.Value(?*Configurator).init(null);
var TYPED_STORAGE = std.atomic.Value(?*TypedStorage).init(null);
var TASK_QUEUE = std.atomic.Value(?*TaskQueue).init(null);
var METRICS_AGGREGATOR = std.atomic.Value(?*MetricsAggregator).init(null);

pub inline fn loadConfiguration(configurator: *Configurator) void {
    _ = CONFIGURATOR.cmpxchgStrong(null, configurator, .acq_rel, .monotonic);
}

pub inline fn init(graph_storage: *TypedStorage, task_queue: *TaskQueue, metrics_aggregator: *MetricsAggregator) void {
    _ = TYPED_STORAGE.cmpxchgStrong(null, graph_storage, .acq_rel, .monotonic);
    _ = TASK_QUEUE.cmpxchgStrong(null, task_queue, .acq_rel, .monotonic);
    _ = METRICS_AGGREGATOR.cmpxchgStrong(null, metrics_aggregator, .acq_rel, .monotonic);
}

pub inline fn getConfigurator() ?*Configurator {
    return CONFIGURATOR.load(.acquire);
}

pub inline fn getTypedStorage() ?*TypedStorage {
    return TYPED_STORAGE.load(.acquire);
}

pub inline fn getTaskQueue() ?*TaskQueue {
    return TASK_QUEUE.load(.acquire);
}

pub inline fn getMetricsAggregator() ?*MetricsAggregator {
    return METRICS_AGGREGATOR.load(.acquire);
}

// Testing helpers
const testing = std.testing;
const TestingConfigurator = @import("./configurator.zig").TestingConfigurator;

pub fn deinitConfigurationForTests() void {
    if (getConfigurator()) |conf| {
        const ptr: *TestingConfigurator = @ptrCast(@alignCast(conf.ptr));
        testing.allocator.destroy(ptr);
        CONFIGURATOR.store(null, .release);
    }
}

pub inline fn initTaskQueueForTests(task_queue: *TaskQueue) void {
    _ = TASK_QUEUE.cmpxchgStrong(null, task_queue, .seq_cst, .seq_cst);
}

pub inline fn cleanAndDeinitTaskQueueForTests() void {
    if (getTaskQueue()) |queue| {
        while (queue.dequeue()) |task| {
            task.destroy(testing.allocator);
        }
        queue.deinit();
        TASK_QUEUE.store(null, .release);
    }
}
