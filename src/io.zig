const std = @import("std");
const posix = std.posix;
const query = @import("./query.zig");
const global_context = @import("./global_context.zig");
const Task = @import("./task_queue.zig").Task;

const DEFAULT_PORT: u16 = 7733;

pub fn run_io_worker() void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    var io = IO.init(gpa.allocator()) catch {
        std.log.err("Error! Failed to start io worker.\n", .{});
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
        RequestTooLong,
        QueryMalformed,
        QueryExecutionError,
    };

    pub fn Response(comptime T: type) type {
        return struct {
            data: T,
            errors: ?IoError,
        };
    }

    pub const IoTask = struct {
        allocator: std.mem.Allocator,
        client_address: std.net.Address,
        socket: posix.socket_t,

        fn run(ptr: *anyopaque) void {
            const self: *IoTask = @ptrCast(@alignCast(ptr));
            defer {
                posix.close(self.socket);
            }
            const graph_storage = global_context.get_graph_storage();
            if (graph_storage == null) {
                std.log.err("Error! Graph storage is not initialized.\n", .{});
                return;
            }
            var buffer: [4096]u8 = [_]u8{0} ** 4096;

            const bytes_read = posix.read(self.socket, &buffer) catch |e| {
                std.log.err("Error! Failed to read a message: {any}\n", .{e});
                self.send_response(i8, -1, IoError.RequestReadingError);
                return;
            };

            if (bytes_read > 0 and bytes_read <= buffer.len) {
                const result: u64 = query.exec(graph_storage.?, buffer[0..bytes_read]) catch |e| {
                    std.log.err("Error! Query execution failed: {any}\n", .{e});
                    switch (e) {
                        query.QueryError.UnknownOperation => {
                            self.send_response(i8, -1, IoError.QueryMalformed);
                        },
                        else => {
                            self.send_response(i8, -1, IoError.QueryExecutionError);
                        },
                    }
                    return;
                };
                self.send_response(u64, result, null);
            } else {
                self.send_response(i8, -1, IoError.RequestTooLong);
            }
        }

        fn task(self: *IoTask) Task {
            return .{
                .context = self,
                .runFn = run,
            };
        }

        fn send_response(self: *IoTask, comptime T: type, data: T, err: ?IoError) void {
            const response = Response(T){
                .data = data,
                .errors = err,
            };
            const message = std.json.stringifyAlloc(self.allocator, response, .{}) catch |e| {
                std.log.err("Error! Failed send the response: {any}\n", .{e});
                return;
            };
            defer self.allocator.free(message);
            _ = posix.write(self.socket, message) catch |e| {
                std.log.err("Error! Failed send the response: {any}\n", .{e});
                return;
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
            std.log.err("Error! Failed to configure a socket: {any}\n", .{e});
            return;
        };
        posix.bind(self.socket_handle, &self.address.any, self.address.getOsSockLen()) catch |e| {
            std.log.err("Error! Failed to bind to a socket: {any}\n", .{e});
            return;
        };
        posix.listen(self.socket_handle, 128) catch |e| {
            std.log.err("Error! Failed opening the port for listening: {any}\n", .{e});
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
                std.log.err("Error! Failed to accept connection: {any}\n", .{e});
                continue;
            };

            var io_task = IoTask{
                .allocator = self.allocator,
                .client_address = client_address,
                .socket = socket,
            };

            global_context.get_task_queue().?.enqueue(io_task.task());
        }
    }

    pub fn deinit(self: *const IO) void {
        posix.close(self.socket_handle);
    }
};
