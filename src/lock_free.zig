const std = @import("std");

// S have to be power of 2 so it is possible to use bitwise and to compute modulo
pub fn DestroyBuffer(E: type, S: u64) type {
    return struct {
        head: u64,
        buffer: []?*E,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator) Self {
            var buffer = allocator.alloc(?*E, S) catch unreachable;
            for (0..buffer.len) |i| {
                buffer[i] = null;
            }
            return .{
                .head = 0,
                .buffer = buffer,
            };
        }

        pub inline fn put(self: *Self, entry: *E) ?*E {
            const index = @atomicRmw(u64, &self.head, .Add, 1, .acq_rel);
            const to_delete = @atomicRmw(?*E, &self.buffer[index % S], .Xchg, entry, .acq_rel);
            _ = @atomicRmw(u64, &self.head, .And, S - 1, .acq_rel);
            return to_delete;
        }
    };
}

// Testing
const testing = std.testing;

test "DestroyBuffer" {
    var destroy_buffer = DestroyBuffer(u8, 2).init(testing.allocator);
    defer {
        testing.allocator.free(destroy_buffer.buffer);
    }

    try testing.expect(destroy_buffer.head == 0);
    try testing.expect(destroy_buffer.buffer[0] == null);

    var t1: u8 = 0;
    const r1 = destroy_buffer.put(&t1);
    try testing.expect(destroy_buffer.head == 1);
    try testing.expect(r1 == null);
    try testing.expect(destroy_buffer.buffer[0].?.* == 0);

    var t2: u8 = 1;
    const r2 = destroy_buffer.put(&t2);
    try testing.expect(destroy_buffer.head == 0);
    try testing.expect(r2 == null);
    try testing.expect(destroy_buffer.buffer[1].?.* == 1);

    var t3: u8 = 2;
    const r3 = destroy_buffer.put(&t3);
    try testing.expect(destroy_buffer.head == 1);
    try testing.expect(r3.?.* == 0);
    try testing.expect(destroy_buffer.buffer[0].?.* == 2);
}
