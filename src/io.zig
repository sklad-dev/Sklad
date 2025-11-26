const std = @import("std");
const posix = std.posix;

const global_context = @import("./global_context.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const QueryProcessingTask = @import("./parse.zig").QueryProcessingTask;
const MetricKind = @import("./metrics.zig").MetricKind;
const MetricRequestTask = @import("./metrics.zig").MetricRequestTask;
const recordMetric = @import("./metrics.zig").recordMetric;
const Task = @import("./task_queue.zig").Task;

pub const DEFAULT_PORT: u16 = 7733;

pub fn runIoWorker() void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    var io = IO.init(gpa.allocator()) catch {
        std.log.err("Error! Failed to start io worker", .{});
        return;
    };
    defer io.deinit();

    io.listen();
}

pub const IO = struct {
    allocator: std.mem.Allocator,
    address: std.net.Address,
    socket_handle: posix.socket_t,

    pub const IoError = error{
        RequestReadingError,
        RequestProcessingError,
        RequestTooLong,
        QueryMalformed,
        QueryExecutionError,
        ProcessingTimeout,
    };

    pub fn Response(comptime T: type, comptime E: type) type {
        return struct {
            data: T,
            errors: ?E,
        };
    }

    const Request = struct {
        kind: RequestKind,
        query: []u8,
        timestamp: i64,
    };

    const RequestKind = enum(u8) {
        metric,
        query,
    };

    pub const IoContext = struct {
        address: std.net.Address,
        socket: std.posix.socket_t,
        start_time: i64,

        pub fn sendResponse(self: *const IoContext, comptime T: type, comptime E: type, allocator: std.mem.Allocator, data: T, err: ?E) void {
            const response = Response(T, E){
                .data = data,
                .errors = err,
            };

            self.doSend(T, E, allocator, &response) catch |e| {
                std.log.err("Error! Failed send the response: {any}", .{e});
                std.posix.close(self.socket);
                return;
            };
        }

        fn doSend(self: *const IoContext, comptime T: type, comptime E: type, allocator: std.mem.Allocator, response: *const Response(T, E)) !void {
            var writer = std.Io.Writer.Allocating.init(allocator);
            defer writer.deinit();

            try std.json.Stringify.value(response, .{ .whitespace = .minified }, &writer.writer);
            const message = try writer.toOwnedSlice();
            _ = try posix.write(self.socket, message);
        }
    };

    pub const IoTask = struct {
        allocator: std.mem.Allocator,
        io_context: IoContext,

        fn run(ptr: *anyopaque) void {
            const self: *IoTask = @ptrCast(@alignCast(ptr));
            var buffer: [4096]u8 = [_]u8{0} ** 4096;
            const bytes_read = posix.read(self.io_context.socket, &buffer) catch |e| {
                std.log.err("Error! Failed to read a message: {any}", .{e});
                self.io_context.sendResponse(i8, IoError, self.allocator, -1, IoError.RequestReadingError);
                std.posix.close(self.io_context.socket);
                return;
            };

            if (bytes_read > 0 and bytes_read <= buffer.len) {
                const request = std.json.parseFromSlice(
                    Request,
                    self.allocator,
                    buffer[0..bytes_read],
                    .{},
                ) catch |e| {
                    std.log.err("Error! Failed to parse a request: {any}", .{e});
                    self.io_context.sendResponse(i8, IoError, self.allocator, -1, IoError.RequestProcessingError);
                    std.posix.close(self.io_context.socket);
                    return;
                };
                defer request.deinit();

                const task_queue = global_context.getTaskQueue();

                if (request.value.kind == .metric) {
                    var metric_task = task_queue.?.allocator.create(MetricRequestTask) catch |e| {
                        std.log.err("Error! Failed to allocate a task to process the metric request: {any}", .{e});
                        std.posix.close(self.io_context.socket);
                        return;
                    };
                    metric_task.* = MetricRequestTask.init(
                        task_queue.?.allocator,
                        request.value.timestamp,
                        self.io_context,
                    ) catch |e| {
                        std.log.err("Error! Failed to allocate a task to process the metric request: {any}", .{e});
                        std.posix.close(self.io_context.socket);
                        return;
                    };
                    global_context.getTaskQueue().?.enqueue(metric_task.task());
                } else if (request.value.kind == .query) {
                    var query_task = task_queue.?.allocator.create(QueryProcessingTask) catch |e| {
                        std.log.err("Error! Failed to allocate a task to process the query: {any}", .{e});
                        std.posix.close(self.io_context.socket);
                        return;
                    };
                    query_task.* = QueryProcessingTask.init(
                        task_queue.?.allocator,
                        request.value.query.len,
                        self.io_context,
                    ) catch |e| {
                        std.log.err("Error! Failed to create a task to process the query: {any}", .{e});
                        std.posix.close(self.io_context.socket);
                        return;
                    };

                    @memcpy(query_task.query, request.value.query);

                    global_context.getTaskQueue().?.enqueue(query_task.task());
                }
            } else {
                self.io_context.sendResponse(i8, IoError, self.allocator, -1, IoError.RequestTooLong);
            }
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *IoTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn task(self: *IoTask) Task {
            return .{
                .context = self,
                .run_fn = run,
                .destroy_fn = destroy,
                .enqued_at = std.time.microTimestamp(),
            };
        }
    };

    pub fn init(allocator: std.mem.Allocator) !IO {
        const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, DEFAULT_PORT);
        const socket_handle = try posix.socket(
            address.any.family,
            posix.SOCK.STREAM,
            posix.IPPROTO.TCP,
        );

        return IO{
            .allocator = allocator,
            .address = address,
            .socket_handle = socket_handle,
        };
    }

    pub fn listen(self: *IO) void {
        posix.setsockopt(
            self.socket_handle,
            posix.SOL.SOCKET,
            posix.SO.REUSEADDR,
            &std.mem.toBytes(@as(c_int, 1)),
        ) catch |e| {
            std.log.err("Error! Failed to configure a socket: {any}", .{e});
            return;
        };
        posix.bind(self.socket_handle, &self.address.any, self.address.getOsSockLen()) catch |e| {
            std.log.err("Error! Failed to bind to a socket: {any}", .{e});
            return;
        };
        posix.listen(self.socket_handle, 128) catch |e| {
            std.log.err("Error! Failed opening the port for listening: {any}", .{e});
            return;
        };

        while (true) {
            var client_address: std.net.Address = undefined;
            var client_address_len: posix.socklen_t = @sizeOf(std.net.Address);

            const socket = posix.accept(
                self.socket_handle,
                &client_address.any,
                &client_address_len,
                0,
            ) catch |e| {
                std.log.err("Error! Failed to accept connection: {any}", .{e});
                continue;
            };

            recordMetric(global_context.getMetricsAggregator(), MetricKind.requestCounter, 1);

            const start_time = std.time.microTimestamp();
            const task_queue = global_context.getTaskQueue();
            var io_task = task_queue.?.allocator.create(IoTask) catch |e| {
                std.log.err("Error! Failed to allocate an IO task: {any}", .{e});
                std.posix.close(socket);
                continue;
            };

            io_task.* = IoTask{
                .allocator = self.allocator,
                .io_context = .{
                    .address = client_address,
                    .socket = socket,
                    .start_time = start_time,
                },
            };

            global_context.getTaskQueue().?.enqueue(io_task.task());
        }
    }

    pub fn deinit(self: *const IO) void {
        posix.close(self.socket_handle);
    }
};
