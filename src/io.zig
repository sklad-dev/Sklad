const std = @import("std");
const posix = std.posix;
const query = @import("./query.zig");
const GraphStorage = @import("./graph_storage.zig").GraphStorage;

const DEFAULT_PORT: u16 = 7733;

pub const IO = struct {
    allocator: std.mem.Allocator,
    address: std.net.Address,
    socket_handle: posix.socket_t,
    stream: std.net.Stream,
    graph_storage: GraphStorage,

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

    pub fn init(allocator: std.mem.Allocator) !IO {
        const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, DEFAULT_PORT);
        const socket_handle = try posix.socket(
            address.any.family,
            posix.SOCK.STREAM,
            posix.IPPROTO.TCP,
        );

        const stream = std.net.Stream{ .handle = socket_handle };

        return IO{
            .allocator = allocator,
            .address = address,
            .socket_handle = socket_handle,
            .stream = stream,
            .graph_storage = try GraphStorage.init(allocator, 8, 8),
        };
    }

    pub fn listen(self: *IO) void {
        const stdout = std.io.getStdOut().writer();
        var server = self.address.listen(.{}) catch {
            stdout.print("Error! Failed opening the port for listening.\n", .{}) catch {
                return;
            };
            return;
        };
        var buffer: [2048]u8 = [_]u8{0} ** 2048;
        while (true) {
            const connection = server.accept() catch {
                stdout.print("Error! Failed to accept connection.\n", .{}) catch {
                    continue;
                };
                continue;
            };
            defer connection.stream.close();

            const reader = connection.stream.reader();
            const bytes_read = reader.read(&buffer) catch {
                self.send_response(i8, connection.stream, -1);
                continue;
            };

            if (bytes_read > 0 and bytes_read <= buffer.len) {
                const result: u64 = query.exec(&self.graph_storage, buffer[0..bytes_read]) catch |err| {
                    switch (err) {
                        query.QueryError.UnknownOperation => {
                            self.send_response(i8, connection.stream, -1);
                        },
                        else => {
                            self.send_response(i8, connection.stream, -1);
                        },
                    }
                    continue;
                };
                self.send_response(u64, connection.stream, result);
            } else {
                self.send_response(i8, connection.stream, -1);
            }
        }
    }

    pub fn deinit(self: *const IO) void {
        posix.close(self.socket_handle);
    }

    fn send_response(self: *IO, comptime T: type, stream: std.net.Stream, data: T) void {
        const response = Response(T){
            .data = data,
            .errors = IoError.RequestTooLong,
        };
        const message = std.json.stringifyAlloc(self.allocator, response, .{}) catch {
            const stdout = std.io.getStdOut().writer();
            stdout.print("Error! Failed send the response.\n", .{}) catch return;
            return;
        };
        defer self.allocator.free(message);
        _ = stream.write(message) catch return;
    }
};
