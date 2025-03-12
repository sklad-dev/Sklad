const std = @import("std");
const posix = std.posix;

const query = @import("./query.zig");
const global_context = @import("./global_context.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const Task = @import("./task_queue.zig").Task;
const LexerTask = @import("./lex.zig").LexerTask;
const Token = @import("./lex.zig").Token;

pub const DEFAULT_PORT: u16 = 7733;

pub fn run_io_worker() void {
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

    pub const Request = struct {
        command: []u8,
    };

    pub const IoContext = struct {
        address: std.net.Address,
        socket: std.posix.socket_t,

        pub fn send_response(self: *const IoContext, comptime T: type, comptime E: type, allocator: std.mem.Allocator, data: T, err: ?E) void {
            const response = Response(T, E){
                .data = data,
                .errors = err,
            };
            const message = std.json.stringifyAlloc(allocator, response, .{}) catch |e| {
                std.log.err("Error! Failed send the response: {any}", .{e});
                return;
            };
            defer allocator.free(message);
            _ = posix.write(self.socket, message) catch |e| {
                std.log.err("Error! Failed send the response: {any}", .{e});
                return;
            };
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
                self.io_context.send_response(i8, IoError, self.allocator, -1, IoError.RequestReadingError);
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
                    self.io_context.send_response(i8, IoError, self.allocator, -1, IoError.RequestProcessingError);
                    return;
                };
                defer request.deinit();

                const task_queue = global_context.get_task_queue();
                var lexer_task = task_queue.?.allocator.create(LexerTask) catch |e| {
                    std.log.err("Error! Failed to allocate a lexer task: {any}", .{e});
                    return;
                };
                lexer_task.* = LexerTask.init(
                    task_queue.?.allocator,
                    request.value.command.len,
                    self.io_context,
                ) catch |e| {
                    std.log.err("Error! Failed to create a lexer task: {any}", .{e});
                    return;
                };

                @memcpy(lexer_task.query, request.value.command);
                lexer_task.tokens.* = std.ArrayList(Token).init(task_queue.?.allocator);

                global_context.get_task_queue().?.enqueue(lexer_task.task());
            } else {
                self.io_context.send_response(i8, IoError, self.allocator, -1, IoError.RequestTooLong);
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

            const task_queue = global_context.get_task_queue();
            var io_task = task_queue.?.allocator.create(IoTask) catch |e| {
                std.log.err("Error! Failed to allocate an IO task: {any}", .{e});
                continue;
            };

            io_task.* = IoTask{
                .allocator = self.allocator,
                .io_context = .{
                    .address = client_address,
                    .socket = socket,
                },
            };

            global_context.get_task_queue().?.enqueue(io_task.task());
        }
    }

    pub fn deinit(self: *const IO) void {
        posix.close(self.socket_handle);
    }
};
