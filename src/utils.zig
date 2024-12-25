const std = @import("std");

pub inline fn generate_id(rng: std.Random) [2]u8 {
    var buf: [2]u8 = undefined;
    rng.bytes(&buf);
    return buf;
}

pub inline fn write_number(comptime T: type, file: std.fs.File, number: T) !void {
    try file.writer().writeInt(T, number, std.builtin.Endian.big);
}

pub inline fn read_number(comptime T: type, file: std.fs.File) !T {
    const value: T = try file.reader().readInt(T, std.builtin.Endian.big);
    return value;
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

pub inline fn num_digits(comptime T: type, number: T) u8 {
    if (number == 0) return 1;

    var digits: u8 = 0;
    var n = number;
    while (n != 0) {
        n = @divTrunc(n, 10);
        digits += 1;
    }
    return digits;
}

pub fn make_dir_if_not_exists(dir_path: []const u8) !void {
    std.fs.cwd().makeDir(dir_path) catch |e| brk: {
        if (e == error.PathAlreadyExists) {
            break :brk;
        } else {
            return e;
        }
    };
}

// Tests
const testing = std.testing;

test "compare_bitwise" {
    // Case 1: empty arrays
    const a1 = [_]u8{};
    const a2 = [_]u8{};
    try testing.expect(compare_bitwise(&a1, &a2) == 0);

    // Case 2: arrays of zero of different size
    const a3 = [1]u8{0};
    const a4 = [2]u8{ 0, 0 };
    try testing.expect(compare_bitwise(&a3, &a4) < 0);
    try testing.expect(compare_bitwise(&a4, &a3) > 0);

    // Case 3: empty array vs array of zero
    try testing.expect(compare_bitwise(&a1, &a3) < 0);

    // Case 4: arrays of zero of the same size
    const a5 = [2]u8{ 0, 0 };
    try testing.expect(compare_bitwise(&a4, &a5) == 0);

    // Case 5: arrays of the same size
    const a6 = [4]u8{ 0, 0, 0, 0 };
    const a7 = [4]u8{ 0, 0, 0, 1 };
    const a8 = [4]u8{ 0, 0, 0, 2 };
    try testing.expect(compare_bitwise(&a6, &a7) < 0);
    try testing.expect(compare_bitwise(&a7, &a8) < 0);

    // Case 6: arrays of different size
    const a9 = [2]u8{ 0, 1 };
    try testing.expect(compare_bitwise(&a9, &a6) > 0);
    try testing.expect(compare_bitwise(&a4, &a7) < 0);
}
