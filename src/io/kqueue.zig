const std = @import("std");
const posix = std.posix;

pub fn Queue(comptime SIZE: u64) type {
    return struct {
        const Self = @This();

        descriptor: i32 = -1,
        event_list: [SIZE]posix.system.Kevent = undefined,

        pub inline fn initialize(self: *Self) !void {
            self.descriptor = try posix.kqueue();
        }

        pub inline fn deinit(self: *const Self) void {
            posix.close(self.descriptor);
        }

        pub inline fn wait(self: *Self) u64 {
            return posix.kevent(
                self.descriptor,
                &.{},
                &self.event_list,
                null,
            ) catch |e| {
                std.log.err("Error! Failed to wait for events: {any}", .{e});
                return 0;
            };
        }

        pub inline fn readySocket(event: posix.system.Kevent) i32 {
            return @intCast(event.udata);
        }

        pub inline fn isReadEvent(event: posix.system.Kevent) bool {
            return event.filter == std.posix.system.EVFILT.READ;
        }

        pub inline fn isWriteEvent(event: posix.system.Kevent) bool {
            return event.filter == std.posix.system.EVFILT.WRITE;
        }

        pub inline fn addSocketReadEvent(self: *const Self, socket: posix.socket_t) !void {
            _ = try posix.kevent(self.descriptor, &.{.{
                .ident = @intCast(socket),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.ADD | posix.system.EV.DISPATCH | posix.system.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = @intCast(socket),
            }}, &.{}, null);
        }

        pub inline fn enableSocketReadEvent(self: *const Self, socket: posix.socket_t) !void {
            _ = try posix.kevent(self.descriptor, &.{.{
                .ident = @intCast(socket),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.ENABLE,
                .fflags = 0,
                .data = 0,
                .udata = @intCast(socket),
            }}, &.{}, null);
        }

        pub inline fn disableSocketReadEvent(self: *const Self, socket: posix.socket_t) !void {
            _ = try posix.kevent(self.descriptor, &.{.{
                .ident = @intCast(socket),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.DISABLE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            }}, &.{}, null);
        }

        pub inline fn enableSocketWriteEvent(self: *const Self, socket: posix.socket_t, has_written: bool) !void {
            var flags: u16 = undefined;
            if (has_written) {
                flags = posix.system.EV.ENABLE;
            } else {
                flags = posix.system.EV.ADD | posix.system.EV.DISPATCH | posix.system.EV.CLEAR;
            }

            _ = try posix.kevent(self.descriptor, &.{.{
                .ident = @intCast(socket),
                .filter = posix.system.EVFILT.WRITE,
                .flags = flags,
                .fflags = 0,
                .data = 0,
                .udata = @intCast(socket),
            }}, &.{}, null);
        }

        pub inline fn disableSocketWriteEvent(self: *const Self, socket: posix.socket_t) !void {
            _ = try posix.kevent(self.descriptor, &.{.{
                .ident = @intCast(socket),
                .filter = posix.system.EVFILT.WRITE,
                .flags = posix.system.EV.DISABLE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            }}, &.{}, null);
        }

        pub inline fn addSocketEvent(self: *const Self, socket: posix.socket_t) !void {
            _ = try posix.kevent(self.descriptor, &.{.{
                .ident = @intCast(socket),
                .filter = posix.system.EVFILT.READ,
                .flags = posix.system.EV.ADD,
                .fflags = 0,
                .data = 0,
                .udata = @intCast(socket),
            }}, &.{}, null);
        }
    };
}
