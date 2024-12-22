const std = @import("std");
const posix = std.posix;

const DEFAULT_PORT: u16 = 7733;

pub const IO = struct {
    address: std.net.Address,
    socket_handle: posix.socket_t,
    stream: std.net.Stream,

    pub fn init() !IO {
        const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, DEFAULT_PORT);
        const socket_handle = try posix.socket(
            address.any.family,
            posix.SOCK.STREAM,
            posix.IPPROTO.TCP,
        );

        const stream = std.net.Stream{ .handle = socket_handle };

        return IO{
            .address = address,
            .socket_handle = socket_handle,
            .stream = stream,
        };
    }

    pub fn listen(self: *const IO) void {
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
            _ = reader.read(&buffer) catch {
                stdout.print("Error! Failed reading request.\n", .{}) catch {
                    return;
                };
                return;
            };
            _ = connection.stream.write(&buffer) catch {
                stdout.print("Error! Failed writing response.\n", .{}) catch {
                    return;
                };
                return;
            };
        }
    }

    pub fn deinit(self: *const IO) void {
        posix.close(self.socket_handle);
    }
};
