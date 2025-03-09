const std = @import("std");
const utils = @import("./utils.zig");
const File = std.fs.File;
const Allocator = std.mem.Allocator;

pub const ValueType = enum(u8) {
    boolean, //bool
    smallint, // i8
    int, // i32
    bigint, // i64
    smallserial, // u8
    serial, // u32
    bigserial, // u64
    float, // f32
    bigfloat, //f64
    string, // variable length string

    pub fn from_bytes(bytes: []const u8) ValueType {
        return @enumFromInt(utils.int_from_bytes(u8, bytes, 0));
    }
};

pub const BinaryData = []const u8;

pub const TypedBinaryData = struct {
    allocator: std.mem.Allocator,
    data_type: ValueType,
    data: BinaryData,

    pub fn from_bytes(allocator: std.mem.Allocator, source: []const u8) !TypedBinaryData {
        const result = try allocator.alloc(u8, source.len - 1);
        @memcpy(result, source[1..]);
        return .{
            .allocator = allocator,
            .data_type = ValueType.from_bytes(source),
            .data = result,
        };
    }
};

pub const StorageRecord = struct {
    allocator: std.mem.Allocator,
    key_size: u16,
    key: BinaryData,
    value_size: u16,
    value: BinaryData,

    pub fn write(self: *const StorageRecord, file: File) !void {
        try utils.write_number(@TypeOf(self.key_size), file, self.key_size);
        try file.writeAll(self.key);
        try utils.write_number(@TypeOf(self.value_size), file, self.value_size);
        try file.writeAll(self.value);
    }

    pub fn destroy(self: *const StorageRecord) void {
        self.allocator.free(self.key);
        self.allocator.free(self.value);
    }

    pub fn read(allocator: Allocator, file: File) !StorageRecord {
        const key_size: u16 = try utils.read_number(u16, file);
        const key: []u8 = try allocator.alloc(u8, key_size);
        _ = try file.read(key[0..]);
        const value_size: u16 = try utils.read_number(u16, file);
        const value: []u8 = try allocator.alloc(u8, value_size);
        _ = try file.read(value[0..]);
        return .{
            .allocator = allocator,
            .key_size = key_size,
            .key = key,
            .value_size = value_size,
            .value = value,
        };
    }

    pub inline fn read_from_offset(allocator: Allocator, file: File, offset: u32) !StorageRecord {
        try file.seekTo(@intCast(offset));
        const result = try read(allocator, file);
        return result;
    }
};
