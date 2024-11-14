const std = @import("std");

pub inline fn generate_id(rng: std.Random) [2]u8 {
    var buf: [2]u8 = undefined;
    rng.bytes(&buf);
    return buf;
}

pub inline fn key_from_int_data(comptime T: type, key_value: T) [@sizeOf(T)]u8 {
    var buffer: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buffer, key_value, std.builtin.Endian.big);
    return buffer;
}

pub fn compare_bitwise(v1: []const u8, v2: []const u8) isize {
    if (v1.len == v2.len and std.mem.eql(u8, v1, v2)) return 0;

    const min_length = @min(v1.len, v2.len);
    for (0..min_length) |i| {
        if (v1[i] != v2[i]) {
            return @as(isize, @intCast(v1[i])) - @as(isize, @intCast(v2[i]));
        }
    }

    return @as(isize, @intCast(v1.len)) - @as(isize, @intCast(v2.len));
}
