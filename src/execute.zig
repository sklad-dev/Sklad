const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const parse = @import("./parse.zig");
const utils = @import("./utils.zig");

const MetricKind = @import("./metrics.zig").MetricKind;
const Task = @import("./task_queue.zig").Task;
const TypedBinaryData = @import("./data_types.zig").TypedBinaryData;
const TypedStorage = @import("./typed_storage.zig").TypedStorage;
const ValueType = @import("./data_types.zig").ValueType;

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
            .enqued_at = std.time.microTimestamp(),
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        defer {
            const exec_time = std.time.microTimestamp() - self.io_context.start_time;
            _ = global_context.getMetricsAggregator().?.record(.{
                .timestamp = std.time.microTimestamp(),
                .value = @intCast(exec_time),
                .kind = @intFromEnum(MetricKind.requestProcessingTime),
            });
            std.posix.close(self.io_context.socket);
        }

        self.executor.execute(&self.expression) catch |e| {
            std.log.err("Error! Query execution failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.sendResponse(i8, ExecutionError, self.allocator, -1, ExecutionError.ExecutionFailed);
        };
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        switch (self.expression) {
            .set => self.expression.set.destroy(),
            .get => self.expression.get.destroy(),
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
            .storage = global_context.getTypedStorage().?,
        };
    }

    pub fn deinit(self: *Executor) void {
        _ = self;
    }

    pub fn execute(self: *Executor, expression: *parse.Expression) !void {
        switch (expression.*) {
            .set => try self.executeSetExpression(&expression.*.set),
            .get => try self.executeGetExpression(&expression.*.get),
        }
    }

    fn executeSetExpression(self: *Executor, expression: *parse.SetExpression) !void {
        for (expression.pairs.items) |pair| {
            try self.storage.set(pair.key.value, pair.value.value);
        }
        self.io_context.sendResponse(i8, ExecutionError, self.allocator, 0, null);
    }

    fn executeGetExpression(self: *Executor, expression: *parse.GetExpression) !void {
        const result = try self.storage.get(expression.key.value);
        if (result) |r| {
            defer r.allocator.free(r.data);
            self.sendGetResult(r);
        } else {
            self.io_context.sendResponse(i8, ExecutionError, self.allocator, -1, null);
        }
    }

    fn sendGetResult(self: *const Executor, result: TypedBinaryData) void {
        switch (result.data_type) {
            .boolean => {
                const v = utils.intFromBytes(u8, result.data, 0);
                self.io_context.sendResponse([]const u8, ExecutionError, self.allocator, if (v == 1) "true" else "false", null);
            },
            .smallint => {
                const v = utils.intFromBytes(i8, result.data, 0);
                self.io_context.sendResponse(i8, ExecutionError, self.allocator, v, null);
            },
            .int => {
                const v = utils.intFromBytes(i32, result.data, 0);
                self.io_context.sendResponse(i32, ExecutionError, self.allocator, v, null);
            },
            .bigint => {
                const v = utils.intFromBytes(i64, result.data, 0);
                self.io_context.sendResponse(i64, ExecutionError, self.allocator, v, null);
            },
            .smallserial => {
                const v = utils.intFromBytes(u8, result.data, 0);
                self.io_context.sendResponse(u8, ExecutionError, self.allocator, v, null);
            },
            .serial => {
                const v = utils.intFromBytes(u32, result.data, 0);
                self.io_context.sendResponse(u32, ExecutionError, self.allocator, v, null);
            },
            .bigserial => {
                const v = utils.intFromBytes(u64, result.data, 0);
                self.io_context.sendResponse(u64, ExecutionError, self.allocator, v, null);
            },
            .float => {
                const v = utils.intFromBytes(u32, result.data, 0);
                self.io_context.sendResponse(f32, ExecutionError, self.allocator, @as(f32, @bitCast(v)), null);
            },
            .bigfloat => {
                const v = utils.intFromBytes(u64, result.data, 0);
                self.io_context.sendResponse(f64, ExecutionError, self.allocator, @as(f64, @bitCast(v)), null);
            },
            .string => {
                self.io_context.sendResponse([]const u8, ExecutionError, self.allocator, result.data, null);
            },
        }
    }
};
