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
