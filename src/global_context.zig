const std = @import("std");

const GraphStorage = @import("./graph_storage.zig").GraphStorage;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

var GRAPH_STORAGE = std.atomic.Value(?*GraphStorage).init(null);
var TASK_QUEUE = std.atomic.Value(?*TaskQueue).init(null);

pub inline fn init(graph_storage: *GraphStorage, task_queue: *TaskQueue) void {
    _ = GRAPH_STORAGE.cmpxchgStrong(null, graph_storage, .seq_cst, .seq_cst);
    _ = TASK_QUEUE.cmpxchgStrong(null, task_queue, .seq_cst, .seq_cst);
}

pub inline fn get_graph_storage() ?*GraphStorage {
    return GRAPH_STORAGE.load(.acquire);
}

pub inline fn get_task_queue() ?*TaskQueue {
    return TASK_QUEUE.load(.acquire);
}
