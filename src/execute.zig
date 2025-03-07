const std = @import("std");

const global_context = @import("./global_context.zig");
const TypedStorage = @import("./typed_storage.zig").TypedStorage;
const io = @import("./io.zig");
const parse = @import("./parse.zig");
const utils = @import("./utils.zig");
const Task = @import("./task_queue.zig").Task;

pub const ExecutionError = error{
    ExecutionFailed,
    NodeNotFound,
};

pub const ExecuteTask = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    query: []u8,
    expression: parse.Expression,
    executor: Executor,

    pub fn init(allocator: std.mem.Allocator, io_context: io.IO.IoContext, query: []u8, expression: parse.Expression) !ExecuteTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .query = query,
            .expression = expression,
            .executor = Executor.init(allocator, io_context),
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

        self.executor.execute(&self.expression) catch |e| {
            std.log.err("Error! Query execution failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.send_response(i8, ExecutionError, self.allocator, -1, ExecutionError.ExecutionFailed);
        };

        self.io_context.send_response(i8, ExecutionError, self.allocator, 0, null);
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        switch (self.expression) {
            .set => self.expression.set.destory(),
            .get => self.expression.get.destory(),
        }
        self.executor.deinit();
        allocator.free(self.query);
        allocator.destroy(self);
    }
};

const Executor = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    storage: *TypedStorage,

    pub fn init(allocator: std.mem.Allocator, io_context: io.IO.IoContext) Executor {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .storage = global_context.get_typed_storage().?,
        };
    }

    pub fn deinit(self: *Executor) void {
        _ = self;
    }

    pub fn execute(self: *Executor, expression: *parse.Expression) !void {
        switch (expression.*) {
            .set => try self.execute_set_expression(&expression.*.set),
            .get => try self.execute_get_expression(&expression.*.get),
        }
    }

    fn execute_set_expression(self: *Executor, expression: *parse.SetExpression) !void {
        for (expression.pairs.items) |pair| {
            try self.storage.set(pair.key.value, pair.value.value);
        }
    }

    fn execute_get_expression(self: *Executor, expression: *parse.GetExpression) !void {
        const result = try self.storage.get(expression.key.value);
        if (result) |r| {
            defer r.allocator.free(r.data);
            self.io_context.send_response([]const u8, ExecutionError, self.allocator, r.data, null);
        } else {
            self.io_context.send_response(?u8, ExecutionError, self.allocator, null, null);
        }
    }
};
