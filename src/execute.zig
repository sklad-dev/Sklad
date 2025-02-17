const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const parse = @import("./parse.zig");
const utils = @import("./utils.zig");
const Task = @import("./task_queue.zig").Task;

pub const ExecutionError = error{
    ExecutionFailed,
};

pub const ExecuteTask = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    query: []u8,
    expression: parse.Expression,

    pub fn init(allocator: std.mem.Allocator, io_context: io.IO.IoContext, query: []u8, expression: parse.Expression) !ExecuteTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .query = query,
            .expression = expression,
        };
    }

    pub fn task(self: *ExecuteTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        defer std.posix.close(self.io_context.socket);

        execute(&self.expression) catch |e| {
            std.log.err("Error! Query execution failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.send_response(i8, ExecutionError, self.allocator, -1, ExecutionError.ExecutionFailed);
        };

        self.io_context.send_response(i8, ExecutionError, self.allocator, 0, null);
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        allocator.free(self.query);
        switch (self.expression) {
            .insert => self.expression.insert.destory(),
            .insert_connection => self.expression.insert_connection.destory(),
        }
        allocator.destroy(self);
    }
};

fn execute(expression: *parse.Expression) !void {
    switch (expression.*) {
        .insert => try execute_insert_expression(&expression.*.insert),
        .insert_connection => {},
    }
}

fn execute_insert_expression(expression: *parse.InsertExpression) !void {
    var graph_storage = global_context.get_graph_storage().?;
    for (expression.nodes.items) |node| {
        var value = node.value;
        switch (node.value_type) {
            .boolean => value = &try utils.to_byte_key(bool, (std.mem.eql(u8, node.value, "true"))),
            .smallint => value = &try utils.to_byte_key(i8, try std.fmt.parseInt(i8, node.value, 10)),
            .int => value = &try utils.to_byte_key(i32, try std.fmt.parseInt(i32, node.value, 10)),
            .bigint => value = &try utils.to_byte_key(i64, try std.fmt.parseInt(i64, node.value, 10)),
            .smallserial => value = &try utils.to_byte_key(u8, try std.fmt.parseInt(u8, node.value, 10)),
            .serial => value = &try utils.to_byte_key(u32, try std.fmt.parseInt(u32, node.value, 10)),
            .bigserial => value = &try utils.to_byte_key(u64, try std.fmt.parseInt(u64, node.value, 10)),
            .float => value = &try utils.to_byte_key(f32, try std.fmt.parseFloat(f32, node.value)),
            .bigfloat => value = &try utils.to_byte_key(f64, try std.fmt.parseFloat(f64, node.value)),
            else => {},
        }
        _ = try graph_storage.node_storage.put(value, node.value_type);
    }
}
