const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;

pub fn Queue(comptime SIZE: u64) type {
    return struct {
        const Self = @This();

        descriptor: i32 = -1,
        event_list: [SIZE]linux.epoll_event = undefined,

        pub inline fn initialize(self: *Self) !void {
            self.descriptor = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
        }

        pub inline fn deinit(self: *const Self) void {
            posix.close(self.descriptor);
        }

        pub inline fn wait(self: *Self) u64 {
            return posix.epoll_wait(self.descriptor, &self.event_list, -1);
        }

        pub inline fn readySocket(event: linux.epoll_event) i32 {
            return @intCast(event.data.fd);
        }

        pub inline fn isReadEvent(event: linux.epoll_event) bool {
            return (event.events & linux.EPOLL.IN) != 0;
        }

        pub inline fn isWriteEvent(event: linux.epoll_event) bool {
            return (event.events & linux.EPOLL.OUT) != 0;
        }

        pub inline fn addSocketReadEvent(self: *const Self, socket: posix.socket_t) !void {
            var event = linux.epoll_event{
                .events = linux.EPOLL.IN | linux.EPOLL.ONESHOT | linux.EPOLL.ET,
                .data = .{ .fd = socket },
            };
            try posix.epoll_ctl(self.descriptor, linux.EPOLL.CTL_ADD, socket, &event);
        }

        pub inline fn enableSocketReadEvent(self: *const Self, socket: posix.socket_t) !void {
            var event = linux.epoll_event{
                .events = linux.EPOLL.IN | linux.EPOLL.ONESHOT | linux.EPOLL.ET,
                .data = .{ .fd = socket },
            };
            try posix.epoll_ctl(self.descriptor, linux.EPOLL.CTL_MOD, socket, &event);
        }

        pub inline fn disableSocketReadEvent(self: *const Self, socket: posix.socket_t) !void {
            var event = linux.epoll_event{
                .events = 0,
                .data = .{ .fd = socket },
            };
            try posix.epoll_ctl(self.descriptor, linux.EPOLL.CTL_MOD, socket, &event);
        }

        pub inline fn enableSocketWriteEvent(self: *const Self, socket: posix.socket_t, _: bool) !void {
            var event = linux.epoll_event{
                .events = linux.EPOLL.OUT | linux.EPOLL.ONESHOT | linux.EPOLL.ET,
                .data = .{ .fd = socket },
            };
            try posix.epoll_ctl(self.descriptor, linux.EPOLL.CTL_MOD, socket, &event);
        }

        pub inline fn disableSocketWriteEvent(self: *const Self, socket: posix.socket_t) !void {
            var event = linux.epoll_event{
                .events = 0,
                .data = .{ .fd = socket },
            };
            try posix.epoll_ctl(self.descriptor, linux.EPOLL.CTL_MOD, socket, &event);
        }

        pub inline fn addSocketEvent(self: *const Self, socket: posix.socket_t) !void {
            var event = linux.epoll_event{
                .events = linux.EPOLL.IN,
                .data = .{ .fd = socket },
            };
            try posix.epoll_ctl(self.descriptor, linux.EPOLL.CTL_ADD, socket, &event);
        }
    };
}
