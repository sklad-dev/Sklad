const std = @import("std");

const global_context = @import("./global_context.zig");
const thread_pool = @import("./thread_pool.zig");
const io = @import("./io.zig");
const JsonConfigurator = @import("./json_configurator.zig").JsonConfigurator;
const GraphStorage = @import("./graph_storage.zig").GraphStorage;
const TaskQueue = @import("./task_queue.zig").TaskQueue;

const DEFAULT_CONFIGURATION_FILE_PATH = @import("./json_configurator.zig").DEFAULT_CONFIGURATION_FILE_PATH;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    var json_conf = try JsonConfigurator.init(gpa.allocator(), DEFAULT_CONFIGURATION_FILE_PATH);
    var conf = json_conf.configurator();
    global_context.load_configuration(&conf);
    std.log.info("Configuration is loaded", .{});

    var graph_storage = try GraphStorage.init(
        gpa.allocator(),
        conf.memtable_max_size(),
        conf.memtable_max_size(),
    );
    defer graph_storage.stop();
    std.log.info("Storage engine is initialized", .{});

    var task_queue = TaskQueue.init(gpa.allocator());
    defer task_queue.deinit();
    std.log.info("Task queue is initialized", .{});

    global_context.init(&graph_storage, &task_queue);

    var worker_thread = try std.Thread.spawn(.{}, thread_pool.run_task, .{});
    worker_thread.detach();

    const thread = try std.Thread.spawn(.{}, io.run_io_worker, .{});
    std.log.info("Listening port {d}", .{io.DEFAULT_PORT});
    thread.join();
}
