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

    pub fn fromBytes(bytes: []const u8) ValueType {
        return @enumFromInt(utils.intFromBytes(u8, bytes, 0));
    }
};

pub const BinaryData = []const u8;

pub const TypedBinaryData = struct {
    allocator: std.mem.Allocator,
    data_type: ValueType,
    data: BinaryData,

    pub fn fromBytes(allocator: std.mem.Allocator, source: []const u8) !TypedBinaryData {
        const result = try allocator.alloc(u8, source.len - 1);
        @memcpy(result, source[1..]);
        return .{
            .allocator = allocator,
            .data_type = ValueType.fromBytes(source),
            .data = result,
        };
    }

    pub inline fn toBytes(self: *const TypedBinaryData) ![]u8 {
        const buffer = try self.allocator.alloc(u8, 1 + self.data.len);
        buffer[0] = @intFromEnum(self.data_type);
        @memcpy(buffer[1..], self.data);
        return buffer;
    }
};

pub const StorageRecord = struct {
    key: BinaryData,
    value: BinaryData,

    pub const Iterator = struct {
        context: *anyopaque,
        next_fn: *const fn (ctx: *anyopaque) anyerror!?StorageRecord,

        pub fn next(self: *Iterator) anyerror!?StorageRecord {
            return self.next_fn(self.context);
        }
    };

    pub fn write(self: *const StorageRecord, writer: *std.fs.File.Writer) !void {
        try utils.writeSizedValue(writer, self.key);
        try utils.writeSizedValue(writer, self.value);
    }

    pub fn destroy(self: *const StorageRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }

    pub fn read(allocator: Allocator, reader: *std.fs.File.Reader, offset: u32) !StorageRecord {
        try reader.seekTo(offset);
        reader.pos = offset;
        const key_size: u16 = try utils.readNumber(u16, &reader.interface);
        reader.pos = offset + 2;
        const key: []u8 = try allocator.alloc(u8, key_size);
        _ = try reader.read(key[0..]);

        try reader.seekTo(offset);
        reader.pos = offset + key_size + 2;
        const value_size: u16 = try utils.readNumber(u16, &reader.interface);
        reader.pos = offset + key_size + 4;
        const value: []u8 = try allocator.alloc(u8, value_size);
        _ = try reader.read(value[0..]);

        return .{
            .key = key,
            .value = value,
        };
    }
};
