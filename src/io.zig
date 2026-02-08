const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const global_context = @import("./global_context.zig");
const recordMetric = @import("./metrics.zig").recordMetric;

const ApplicationError = @import("./constants.zig").ApplicationError;
const ContinueRangeQueryTask = @import("./execute.zig").ContinueRangeQueryTask;
const MetricKind = @import("./metrics.zig").MetricKind;
const MetricRequestTask = @import("./metrics.zig").MetricRequestTask;
const RangeQueryContext = @import("./range_query_context.zig").RangeQueryContext;
const Task = @import("./task_queue.zig").Task;
const QueryProcessingTask = @import("./parse.zig").QueryProcessingTask;

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
    queue: Queue = Queue{},
    client_contexts: []?*IoContext,

    const BACKLOG_SIZE: i32 = 128;
    const MAX_EVENTS: usize = 128;

    const Queue = switch (builtin.target.os.tag) {
        .linux => @import("./io/epoll.zig").Queue(MAX_EVENTS),
        .macos, .tvos, .watchos, .ios, .freebsd, .netbsd, .openbsd => @import("./io/kqueue.zig").Queue(MAX_EVENTS),
        else => @compileError("IO is not supported for platform"),
    };

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
        continue_batch,
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
        has_written: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        socket: std.posix.socket_t,
        start_time: i64,
        response_buffer: ?[]u8 = null,
        bytes_written: usize = 0,
        range_query_context: ?*RangeQueryContext = null,

        pub inline fn init(io: *IO, socket: std.posix.socket_t, start_time: i64) !IoContext {
            return .{
                .allocator = io.allocator,
                .io = io,
                .socket = socket,
                .start_time = start_time,
                .range_query_context = null,
            };
        }

        pub inline fn cleanBuffer(self: *IoContext) void {
            if (self.response_buffer) |buffer| {
                self.allocator.free(buffer);
                self.response_buffer = null;
            }
            self.bytes_written = 0;
        }

        pub fn cleanupRangeQuery(self: *IoContext) void {
            if (self.range_query_context) |context| {
                context.deinit();
                self.allocator.destroy(context);
                self.range_query_context = null;
            }
        }

        pub fn closeSocket(self: *IoContext) void {
            std.posix.close(self.socket);
            self.cleanBuffer();
            self.cleanupRangeQuery();
            self.state.store(@intFromEnum(IoContextState.closed), .release);
        }

        pub fn toNextState(self: *IoContext) void {
            const current_state: IoContextState = @enumFromInt(self.state.load(.acquire));
            switch (current_state) {
                .idle => {
                    self.disableSocketReadEvent();
                    self.state.store(@intFromEnum(IoContextState.processing), .release);
                },
                .processing => {
                    self.state.store(@intFromEnum(IoContextState.writing), .release);
                    self.enableSocketWriteEvent();
                },
                .writing => {
                    self.disableSocketWriteEvent();
                    self.state.store(@intFromEnum(IoContextState.idle), .release);
                    self.enableSocketReadEvent();
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
            const message = self.response_buffer orelse return;

            while (self.bytes_written < message.len) {
                const n = posix.write(self.socket, message[self.bytes_written..]) catch |e| {
                    if (e == error.WouldBlock) {
                        self.enableSocketWriteEvent();
                        return;
                    }

                    std.log.err("Error! Failed to write response to socket: {any}", .{e});
                    self.closeSocket();
                    return e;
                };
                if (n == 0) {
                    self.closeSocket();
                    return IoError.ConnectionClosed;
                }
                self.bytes_written += n;
            }

            const exec_time = @as(u64, @intCast(std.time.microTimestamp() - self.start_time));
            recordMetric(global_context.getMetricsAggregator(), MetricKind.requestProcessingTime, exec_time);

            self.cleanBuffer();
            self.toNextState();
        }

        pub inline fn disableSocketReadEvent(self: *const IoContext) void {
            self.io.queue.disableSocketReadEvent(self.socket) catch |e| {
                std.log.err("Error! Failed to disable socket read events: {any}", .{e});
            };
        }

        pub inline fn enableSocketReadEvent(self: *const IoContext) void {
            self.io.queue.enableSocketReadEvent(self.socket) catch |e| {
                std.log.err("Error! Failed to enable socket read events: {any}", .{e});
            };
        }

        pub inline fn enableSocketWriteEvent(self: *IoContext) void {
            self.io.queue.enableSocketWriteEvent(self.socket, self.has_written.load(.acquire)) catch |e| {
                std.log.err("Error! Failed to enable socket write events: {any}", .{e});
            };
            self.has_written.store(true, .release);
        }

        pub inline fn disableSocketWriteEvent(self: *const IoContext) void {
            self.io.queue.disableSocketWriteEvent(self.socket) catch |e| {
                std.log.err("Error! Failed to disable socket write events: {any}", .{e});
            };
        }

        inline fn prepareResponse(self: *IoContext, comptime T: type, comptime E: type, response: *const Response(T, E)) !void {
            self.cleanBuffer();

            var writer = std.Io.Writer.Allocating.init(self.allocator);
            defer writer.deinit();

            try std.json.Stringify.value(response, .{ .whitespace = .minified }, &writer.writer);
            try writer.writer.writeByte('\n');
            self.response_buffer = try writer.toOwnedSlice();
        }
    };

    pub const ReadRequestTask = struct {
        allocator: std.mem.Allocator,
        io_context: *IoContext,

        fn run(ptr: *anyopaque) void {
            const self: *ReadRequestTask = @ptrCast(@alignCast(ptr));
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
                    self.io_context.cleanupRangeQuery();

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
                } else if (request.value.kind == .continue_batch) {
                    if (self.io_context.range_query_context != null) {
                        var continue_task = task_queue.?.allocator.create(ContinueRangeQueryTask) catch |e| {
                            std.log.err("Error creating continue task: {any}", .{e});
                            self.io_context.closeSocket();
                            return;
                        };
                        continue_task.* = ContinueRangeQueryTask{
                            .allocator = self.allocator,
                            .io_context = self.io_context,
                        };
                        task_queue.?.enqueue(continue_task.task());
                    } else {
                        self.io_context.enqueueResponse(i8, IO.IoError, -1, IO.IoError.QueryMalformed);
                    }
                    return;
                }
            } else {
                std.log.err("Error! Request size {d} exceeds the maximum allowed size", .{bytes_read});
                self.io_context.enqueueResponse(i8, IoError, -1, IoError.RequestTooLarge);
            }
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *ReadRequestTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn task(self: *ReadRequestTask) Task {
            return .{
                .context = self,
                .run_fn = run,
                .destroy_fn = destroy,
                .enqued_at = std.time.microTimestamp(),
            };
        }
    };

    pub const WriteResponseTask = struct {
        allocator: std.mem.Allocator,
        io_context: *IoContext,

        fn run(ptr: *anyopaque) void {
            const self: *WriteResponseTask = @ptrCast(@alignCast(ptr));
            self.io_context.writeResponse() catch |e| {
                std.log.err("Error! Failed to write response: {any}", .{e});
            };
        }

        fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
            const self: *WriteResponseTask = @ptrCast(@alignCast(ptr));
            allocator.destroy(self);
        }

        fn task(self: *WriteResponseTask) Task {
            return .{
                .context = self,
                .run_fn = run,
                .destroy_fn = destroy,
                .enqued_at = std.time.microTimestamp(),
            };
        }
    };

    pub fn init(allocator: std.mem.Allocator) !IO {
        const address = std.net.Address.initIp4(.{ 0, 0, 0, 0 }, DEFAULT_PORT);
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

        self.queue.initialize() catch |e| {
            std.log.err("Error! Failed to create I/O queue descriptor: {any}", .{e});
            return;
        };

        self.queue.addSocketEvent(self.socket_handle) catch |e| {
            std.log.err("Error! Failed to enable socket listening: {any}", .{e});
            return;
        };

        while (true) {
            const ready_count = self.queue.wait();
            if (ready_count == 0) continue;

            for (self.queue.event_list[0..ready_count]) |event| {
                const ready_socket: i32 = Queue.readySocket(event);
                if (ready_socket == self.socket_handle) {
                    if (!self.addClient()) continue;
                } else {
                    if (Queue.isReadEvent(event)) {
                        if (!self.handleRead(ready_socket)) continue;
                    } else if (Queue.isWriteEvent(event)) {
                        if (!self.handleWrite(ready_socket)) continue;
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
        self.queue.deinit();
        posix.close(self.socket_handle);
    }

    fn addClient(self: *IO) bool {
        const client_socket = posix.accept(
            self.socket_handle,
            null,
            null,
            posix.SOCK.NONBLOCK,
        ) catch |e| {
            std.log.err("Error! Failed to accept connection: {any}", .{e});
            return false;
        };

        const io_context = self.allocator.create(IoContext) catch |e| {
            std.log.err("Error! Failed to allocate IO context: {any}", .{e});
            posix.close(client_socket);
            return false;
        };

        io_context.* = IoContext.init(self, client_socket, 0) catch |e| {
            std.log.err("Error! Failed to create IO context: {any}", .{e});
            posix.close(client_socket);
            return false;
        };

        if (!self.addIoContext(io_context)) {
            std.log.err("Connection limit reached, rejecting socket {d}", .{client_socket});
            io_context.cleanBuffer();
            self.allocator.destroy(io_context);
            posix.close(client_socket);
            return false;
        }

        self.queue.addSocketReadEvent(client_socket) catch |e| {
            io_context.closeSocket();
            std.log.err("Error! Failed to enable socket listening: {any}", .{e});
            return false;
        };

        return true;
    }

    fn handleRead(self: *IO, client_socket: posix.socket_t) bool {
        const start_time = std.time.microTimestamp();

        var io_context = self.getIoContext(client_socket) orelse {
            std.log.err("Error! Failed to find IO context for socket: {d}", .{client_socket});
            return false;
        };

        const state: IoContextState = @enumFromInt(io_context.state.load(.acquire));
        if (state != .idle) return false;

        io_context.toNextState();

        recordMetric(global_context.getMetricsAggregator(), MetricKind.requestCounter, 1);
        io_context.start_time = start_time;

        const task_queue = global_context.getTaskQueue();
        var read_request_task = task_queue.?.allocator.create(ReadRequestTask) catch |e| {
            std.log.err("Error! Failed to allocate a request reading task: {any}", .{e});
            return false;
        };

        read_request_task.* = ReadRequestTask{ .allocator = self.allocator, .io_context = io_context };

        global_context.getTaskQueue().?.enqueue(read_request_task.task());

        return true;
    }

    fn handleWrite(self: *IO, client_socket: posix.socket_t) bool {
        const io_context = self.getIoContext(client_socket) orelse {
            std.log.err("Error! Failed to find IO context for socket: {d}", .{client_socket});
            return false;
        };

        const state: IoContextState = @enumFromInt(io_context.state.load(.acquire));
        if (state != .writing) return false;

        const task_queue = global_context.getTaskQueue();
        var write_response_task = task_queue.?.allocator.create(WriteResponseTask) catch |e| {
            std.log.err("Error! Failed to allocate a response writing task: {any}", .{e});
            return false;
        };

        write_response_task.* = WriteResponseTask{ .allocator = self.allocator, .io_context = io_context };
        global_context.getTaskQueue().?.enqueue(write_response_task.task());
        return true;
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
