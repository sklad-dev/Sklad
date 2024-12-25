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
                    return;
                };
                return;
            };
            defer connection.stream.close();

            const reader = connection.stream.reader();
            const bytes_read = reader.read(&buffer) catch {
                stdout.print("Error! Failed reading request.\n", .{}) catch {
                    return;
                };
                return;
            };

            if (bytes_read > 0 and bytes_read <= buffer.len) {
                const result: u64 = query.exec(&self.graph_storage, buffer[0..bytes_read]) catch |err| blk: {
                    switch (err) {
                        query.QueryError.UnknownOperation => {
                            _ = connection.stream.write(&buffer) catch {
                                stdout.print("Error! Incorrect query.\n", .{}) catch {
                                    break :blk 0xFFFFFFFFFFFFFFFF;
                                };
                                break :blk 0xFFFFFFFFFFFFFFFF;
                            };
                        },
                        else => {
                            _ = connection.stream.write(&buffer) catch {
                                stdout.print("Error! Failed executing request.\n", .{}) catch {
                                    break :blk 0xFFFFFFFFFFFFFFFF;
                                };
                                break :blk 0xFFFFFFFFFFFFFFFF;
                            };
                        },
                    }
                    break :blk 0xFFFFFFFFFFFFFFFF;
                };
                _ = connection.stream.writer().writeInt(u64, result, std.builtin.Endian.big) catch return;
            } else {
                _ = connection.stream.write(&buffer) catch {
                    stdout.print("Error! Request too long\n", .{}) catch {
                        return;
                    };
                    return;
                };
            }
        }
    }

    pub fn deinit(self: *const IO) void {
        posix.close(self.socket_handle);
    }
};
