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
    kqueue_descriptor: i32 = -1,
    event_list: [MAX_EVENTS]posix.system.Kevent = undefined,
    client_contexts: []?*IoContext,

    const BACKLOG_SIZE: i32 = 128;
    const MAX_EVENTS: usize = 128;

    pub const IoError = error{
        RequestReadingError,
        RequestProcessingError,
        RequestTooLarge,
        QueryMalformed,
        QueryExecutionError,
        ProcessingTimeout,
        ConnectionClosed,
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

    pub const IoContextState = enum(u8) {
        idle,
        processing,
        writing,
        closed,
    };

    pub const IoContext = struct {
        allocator: std.mem.Allocator,
        io: *const IO,
        state: std.atomic.Value(u8) = std.atomic.Value(u8).init(@intFromEnum(IoContextState.idle)),
        socket: std.posix.socket_t,
        start_time: i64,
        response_buffer: ?[]u8,

        pub inline fn init(io: *const IO, socket: std.posix.socket_t, start_time: i64) !IoContext {
            return .{
                .allocator = io.allocator,
                .io = io,
                .socket = socket,
                .start_time = start_time,
                .response_buffer = null,
            };
        }

        pub inline fn cleanBuffer(self: *IoContext) void {
            if (self.response_buffer) |buffer| {
                self.allocator.free(buffer);
                self.response_buffer = null;
            }
        }

        pub fn closeSocket(self: *IoContext) void {
            std.posix.close(self.socket);
            self.cleanBuffer();
            self.state.store(@intFromEnum(IoContextState.closed), .release);
        }

        pub fn toNextState(self: *IoContext) void {
            const current_state: IoContextState = @enumFromInt(self.state.load(.acquire));
            switch (current_state) {
                .idle => {
                    self.disableSocketReadEvents();
                    self.state.store(@intFromEnum(IoContextState.processing), .release);
                },
                .processing => {
                    self.enableSocketWriteEvents();
                    self.state.store(@intFromEnum(IoContextState.writing), .release);
                },
                .writing => {
                    self.disableSocketWriteEvents();
                    self.enableSocketReadEvents();
                    self.state.store(@intFromEnum(IoContextState.idle), .release);
                },
                else => {},
            }
        }

        pub fn enqueueResponse(self: *IoContext, comptime T: type, comptime E: type, data: T, err: ?E) void {
            const response = Response(T, E){
                .data = data,
                .errors = err,
            };

            self.prepareResponse(T, E, &response) catch |e| {
                std.log.err("Error! Failed send the response: {any}", .{e});
                self.closeSocket();
                return;
            };

            self.toNextState();
        }

        pub fn writeResponse(self: *IoContext) !void {
            const message = self.response_buffer.?;
            var written: usize = 0;
            while (written < message.len) {
                const n = posix.write(self.socket, message[written..]) catch |e| {
                    self.closeSocket();
                    return e;
                };
                if (n == 0) {
                    self.closeSocket();
                    return IoError.ConnectionClosed;
                }
                written += n;
            }

            self.allocator.free(self.response_buffer.?);
            self.response_buffer = null;
            self.toNextState();
        }

        pub inline fn disableSocketReadEvents(self: *const IoContext) void {
            self.io.disableSocketReadEvents(self.socket) catch |e| {
                std.log.err("Error! Failed to disable socket read events: {any}", .{e});
            };
        }

        pub inline fn enableSocketReadEvents(self: *const IoContext) void {
            self.io.enableSocketReadEvents(self.socket) catch |e| {
                std.log.err("Error! Failed to enable socket read events: {any}", .{e});
            };
        }

        pub inline fn enableSocketWriteEvents(self: *const IoContext) void {
            self.io.enableSocketWriteEvents(self.socket) catch |e| {
                std.log.err("Error! Failed to enable socket write events: {any}", .{e});
            };
        }

        pub inline fn disableSocketWriteEvents(self: *const IoContext) void {
            self.io.disableSocketWriteEvents(self.socket) catch |e| {
                std.log.err("Error! Failed to disable socket write events: {any}", .{e});
            };
        }

        inline fn prepareResponse(self: *IoContext, comptime T: type, comptime E: type, response: *const Response(T, E)) !void {
            var writer = std.Io.Writer.Allocating.init(self.allocator);
            defer writer.deinit();

            try std.json.Stringify.value(response, .{ .whitespace = .minified }, &writer.writer);
            try writer.writer.writeByte('\n');
            self.response_buffer = try writer.toOwnedSlice();
        }
    };

    pub const IoTask = struct {
        allocator: std.mem.Allocator,
        io_context: *IoContext,

        fn run(ptr: *anyopaque) void {
            const self: *IoTask = @ptrCast(@alignCast(ptr));
            var buffer: [4096]u8 = [_]u8{0} ** 4096; // TODO: move the buffer to the worker

            const bytes_read = posix.read(self.io_context.socket, &buffer) catch |e| {
                std.log.err("Error! Failed to read a message: {any}", .{e});
                self.io_context.enqueueResponse(i8, IoError, -1, IoError.RequestReadingError);
                return;
            };

            if (bytes_read == 0) {
                self.io_context.closeSocket();
                return;
            }

            // self.io_context.toNextState();
            if (bytes_read > 0 and bytes_read <= buffer.len) {
                const request = std.json.parseFromSlice(
                    Request,
                    self.allocator,
                    buffer[0..bytes_read],
                    .{},
                ) catch |e| {
                    std.log.err("Error! Failed to parse a request: {any}", .{e});
                    self.io_context.enqueueResponse(i8, IoError, -1, IoError.RequestProcessingError);
                    return;
                };
                defer request.deinit();

                const task_queue = global_context.getTaskQueue();

                if (request.value.kind == .metric) {
                    var metric_task = task_queue.?.allocator.create(MetricRequestTask) catch |e| {
                        std.log.err("Error! Failed to allocate a task to process the metric request: {any}", .{e});
                        self.io_context.closeSocket();
                        return;
                    };
                    metric_task.* = MetricRequestTask.init(
                        task_queue.?.allocator,
                        request.value.timestamp,
                        self.io_context,
                    ) catch |e| {
                        std.log.err("Error! Failed to allocate a task to process the metric request: {any}", .{e});
                        self.io_context.closeSocket();
                        return;
                    };
                    global_context.getTaskQueue().?.enqueue(metric_task.task());
                } else if (request.value.kind == .query) {
                    var query_task = task_queue.?.allocator.create(QueryProcessingTask) catch |e| {
                        std.log.err("Error! Failed to allocate a task to process the query: {any}", .{e});
                        self.io_context.closeSocket();
                        return;
                    };
                    query_task.* = QueryProcessingTask.init(
                        task_queue.?.allocator,
                        request.value.query.len,
                        self.io_context,
                    ) catch |e| {
                        std.log.err("Error! Failed to create a task to process the query: {any}", .{e});
                        self.io_context.closeSocket();
                        return;
                    };

                    @memcpy(query_task.query, request.value.query);

                    global_context.getTaskQueue().?.enqueue(query_task.task());
                }
            } else {
                self.io_context.enqueueResponse(i8, IoError, -1, IoError.RequestTooLarge);
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
            posix.SOCK.STREAM | posix.SOCK.NONBLOCK,
            posix.IPPROTO.TCP,
        );

        const max_connections = global_context.getConfigurator().?.maxConnections();
        const clients_array = try allocator.alloc(?*IoContext, max_connections);
        @memset(clients_array, null);

        return IO{
            .allocator = allocator,
            .address = address,
            .socket_handle = socket_handle,
            .client_contexts = clients_array,
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
        posix.listen(self.socket_handle, BACKLOG_SIZE) catch |e| {
            std.log.err("Error! Failed opening the port for listening: {any}", .{e});
            return;
        };

        self.kqueue_descriptor = posix.kqueue() catch |e| {
            std.log.err("Error! Failed to create kqueue descriptor: {any}", .{e});
            return;
        };

        self.enableSocketReadEvents(self.socket_handle) catch |e| {
            std.log.err("Error! Failed to enable socket listening: {any}", .{e});
            return;
        };

        while (true) {
            const ready_count = posix.kevent(
                self.kqueue_descriptor,
                &.{},
                &self.event_list,
                null,
            ) catch |e| {
                std.log.err("Error! Failed to wait for events: {any}", .{e});
                continue;
            };

            for (self.event_list[0..ready_count]) |event| {
                const ready_socket: i32 = @intCast(event.udata);
                if (ready_socket == self.socket_handle) {
                    const client_socket = posix.accept(
                        self.socket_handle,
                        null,
                        null,
                        posix.SOCK.NONBLOCK,
                    ) catch |e| {
                        std.log.err("Error! Failed to accept connection: {any}", .{e});
                        continue;
                    };

                    const io_context = self.allocator.create(IoContext) catch |e| {
                        std.log.err("Error! Failed to allocate IO context: {any}", .{e});
                        posix.close(client_socket);
                        continue;
                    };

                    io_context.* = IoContext.init(self, client_socket, 0) catch |e| {
                        std.log.err("Error! Failed to create IO context: {any}", .{e});
                        posix.close(client_socket);
                        continue;
                    };

                    if (!self.addIoContext(io_context)) {
                        std.log.info("Connection limit reached, rejecting socket {d}", .{client_socket});
                        io_context.cleanBuffer();
                        self.allocator.destroy(io_context);
                        posix.close(client_socket);
                        continue;
                    }

                    self.enableSocketReadEvents(client_socket) catch |e| {
                        io_context.closeSocket();
                        std.log.err("Error! Failed to enable socket listening: {any}", .{e});
                        return;
                    };
                } else {
                    if (event.filter == std.posix.system.EVFILT.READ) {
                        const start_time = std.time.microTimestamp();

                        var io_context = self.getIoContext(ready_socket) orelse {
                            std.log.err("Error! Failed to find IO context for socket: {d}", .{ready_socket});
                            continue;
                        };

                        const state: IoContextState = @enumFromInt(io_context.state.load(.acquire));
                        if (state != .idle) continue;

                        io_context.toNextState();

                        recordMetric(global_context.getMetricsAggregator(), MetricKind.requestCounter, 1);
                        io_context.start_time = start_time;

                        const task_queue = global_context.getTaskQueue();
                        var io_task = task_queue.?.allocator.create(IoTask) catch |e| {
                            std.log.err("Error! Failed to allocate an IO task: {any}", .{e});
                            continue;
                        };

                        io_task.* = IoTask{ .allocator = self.allocator, .io_context = io_context };

                        global_context.getTaskQueue().?.enqueue(io_task.task());
                    } else if (event.filter == std.posix.system.EVFILT.WRITE) {
                        var io_context = self.getIoContext(ready_socket) orelse {
                            std.log.err("Error! Failed to find IO context for socket: {d}", .{ready_socket});
                            continue;
                        };

                        io_context.writeResponse() catch |e| {
                            std.log.err("Error! Failed to write response: {any}", .{e});
                            continue;
                        };
                    }
                }
            }
        }
    }

    pub fn deinit(self: *const IO) void {
        for (self.client_contexts) |io_context| {
            if (io_context) |context| {
                context.closeSocket();
                context.cleanBuffer();
                self.allocator.destroy(context);
            }
        }
        self.allocator.free(self.client_contexts);
        posix.close(self.kqueue_descriptor);
        posix.close(self.socket_handle);
    }

    pub fn enableSocketReadEvents(self: *const IO, socket: posix.socket_t) !void {
        _ = try posix.kevent(self.kqueue_descriptor, &.{.{
            .ident = @intCast(socket),
            .filter = posix.system.EVFILT.READ,
            .flags = posix.system.EV.ADD | posix.system.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intCast(socket),
        }}, &.{}, null);
    }

    pub fn disableSocketReadEvents(self: *const IO, socket: posix.socket_t) !void {
        _ = try posix.kevent(self.kqueue_descriptor, &.{.{
            .ident = @intCast(socket),
            .filter = posix.system.EVFILT.READ,
            .flags = posix.system.EV.DISABLE,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        }}, &.{}, null);
    }

    pub fn enableSocketWriteEvents(self: *const IO, socket: posix.socket_t) !void {
        _ = try posix.kevent(self.kqueue_descriptor, &.{.{
            .ident = @intCast(socket),
            .filter = posix.system.EVFILT.WRITE,
            .flags = posix.system.EV.ADD | posix.system.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intCast(socket),
        }}, &.{}, null);
    }

    pub fn disableSocketWriteEvents(self: *const IO, socket: posix.socket_t) !void {
        _ = try posix.kevent(self.kqueue_descriptor, &.{.{
            .ident = @intCast(socket),
            .filter = posix.system.EVFILT.WRITE,
            .flags = posix.system.EV.DISABLE,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        }}, &.{}, null);
    }

    fn getIoContext(self: *IO, socket: posix.socket_t) ?*IoContext {
        for (self.client_contexts, 0..) |maybe_context, i| {
            if (maybe_context) |io_context| {
                const state: IoContextState = @enumFromInt(io_context.state.load(.acquire));

                if (state == .closed) {
                    self.allocator.destroy(io_context);
                    self.client_contexts[i] = null;
                    continue;
                }

                if (io_context.socket == socket) {
                    return io_context;
                }
            }
        }
        return null;
    }

    fn addIoContext(self: *IO, io_context: *IoContext) bool {
        for (self.client_contexts, 0..) |maybe_context, i| {
            if (maybe_context) |context| {
                const state: IoContextState = @enumFromInt(context.state.load(.acquire));

                if (state == .closed) {
                    self.allocator.destroy(context);
                    self.client_contexts[i] = io_context;
                    return true;
                }
            } else {
                self.client_contexts[i] = io_context;
                return true;
            }
        }
        return false;
    }
};
