const std = @import("std");

pub const SupportingError = error{
    NotImplemented,
};

pub inline fn generateId(rng: std.Random) [2]u8 {
    var buf: [2]u8 = undefined;
    rng.bytes(&buf);
    return buf;
}

pub inline fn writeNumber(comptime T: type, writer: *std.Io.Writer, number: T) !void {
    try writer.writeInt(T, number, .big);
}

pub inline fn readNumber(comptime T: type, reader: *std.Io.Reader) !T {
    const value: T = try reader.peekInt(T, .big);
    return value;
}

pub inline fn intFromBytes(comptime T: type, buffer: []const u8, offset: usize) T {
    return std.mem.readInt(T, buffer[offset .. offset + @sizeOf(T)][0..@sizeOf(T)], .big);
}

pub inline fn toBytes(comptime T: type, value: T) ![@sizeOf(@TypeOf(value))]u8 {
    return switch (T) {
        bool => intToBytes(u8, @as(u8, if (value == true) 1 else 0)),
        i8, i16, i32, i64, u8, u16, u32, u64 => intToBytes(T, value),
        f32 => intToBytes(u32, @as(u32, @bitCast(value))),
        f64 => intToBytes(u64, @as(u64, @bitCast(value))),
        else => error.DataError,
    };
}

pub inline fn intToBytes(comptime T: type, value: T) [@sizeOf(T)]u8 {
    var buffer: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buffer, value, .big);
    return buffer;
}

pub fn compareBitwise(v1: []const u8, v2: []const u8) isize {
    if (v1.len == v2.len and std.mem.eql(u8, v1, v2)) return 0;

    const min_length = @min(v1.len, v2.len);
    for (0..min_length) |i| {
        if (v1[i] != v2[i]) {
            return @as(isize, @intCast(v1[i])) - @as(isize, @intCast(v2[i]));
        }
    }

    return @as(isize, @intCast(v1.len)) - @as(isize, @intCast(v2.len));
}

pub inline fn numDigits(comptime T: type, number: T) u8 {
    if (number == 0) return 1;

    var digits: u8 = 0;
    var n = number;
    while (n != 0) {
        n = @divTrunc(n, 10);
        digits += 1;
    }
    return digits;
}

pub fn makeDirIfNotExists(dir_path: []const u8) !void {
    std.fs.cwd().makeDir(dir_path) catch |e| brk: {
        if (e == error.PathAlreadyExists) {
            break :brk;
        } else {
            return e;
        }
    };
}

pub fn tryLockFor(lock: *std.Thread.Mutex, timeout: i64) bool {
    const start_at: i64 = std.time.milliTimestamp();
    while (true) {
        if (lock.tryLock()) return true;
        if (std.time.milliTimestamp() - start_at >= timeout) return false;
    }
    return false;
}

// Tests
const testing = std.testing;

test "compareBitwise" {
    // Case 1: empty arrays
    const a1 = [_]u8{};
    const a2 = [_]u8{};
    try testing.expect(compareBitwise(&a1, &a2) == 0);

    // Case 2: arrays of zero of different size
    const a3 = [1]u8{0};
    const a4 = [2]u8{ 0, 0 };
    try testing.expect(compareBitwise(&a3, &a4) < 0);
    try testing.expect(compareBitwise(&a4, &a3) > 0);

    // Case 3: empty array vs array of zero
    try testing.expect(compareBitwise(&a1, &a3) < 0);

    // Case 4: arrays of zero of the same size
    const a5 = [2]u8{ 0, 0 };
    try testing.expect(compareBitwise(&a4, &a5) == 0);

    // Case 5: arrays of the same size
    const a6 = [4]u8{ 0, 0, 0, 0 };
    const a7 = [4]u8{ 0, 0, 0, 1 };
    const a8 = [4]u8{ 0, 0, 0, 2 };
    try testing.expect(compareBitwise(&a6, &a7) < 0);
    try testing.expect(compareBitwise(&a7, &a8) < 0);

    // Case 6: arrays of different size
    const a9 = [2]u8{ 0, 1 };
    try testing.expect(compareBitwise(&a9, &a6) > 0);
    try testing.expect(compareBitwise(&a4, &a7) < 0);
}

test "numDigits" {
    try testing.expect(numDigits(i16, 1) == 1);
    try testing.expect(numDigits(i16, 10) == 2);
    try testing.expect(numDigits(i16, 199) == 3);
}
