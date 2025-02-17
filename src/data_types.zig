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

    pub inline fn to_type(self: ValueType) type {
        return switch (self) {
            .boolean => bool,
            .smallint => i8,
            .int => i32,
            .bigint => i64,
            .smallserial => u8,
            .serial => u32,
            .bigserial => u64,
            .float => f32,
            .bigfloat => f64,
            .string => []const u8,
        };
    }

    pub inline fn is_int(self: ValueType) bool {
        return switch (self) {
            .bool, .float, .bigfloat, .string => false,
            else => true,
        };
    }

    pub inline fn is_float(self: ValueType) bool {
        return switch (self) {
            .float, .bigfloat => true,
            else => false,
        };
    }
};

pub const NodePointer = u64;

// A pointer to a node record stored on disk
pub const FsNodePointer = packed struct {
    padding: u8,
    level_id: u8,
    file_id: u16,
    offset: u32,
};

pub fn StorageRecord(comptime V: type) type {
    return struct {
        key_size: u16,
        key: []const u8,
        value: V,

        const Self = @This();

        pub fn write(self: *const Self, file: File) !void {
            try utils.write_number(@TypeOf(self.key_size), file, self.key_size);
            try file.writeAll(self.key);
            try utils.write_number(V, file, self.value);
        }

        pub fn read(allocator: Allocator, file: File) !Self {
            const key_size: u16 = try utils.read_number(u16, file);
            const key_value: []u8 = try allocator.alloc(u8, key_size);
            _ = try file.read(key_value[0..]);
            const value = try utils.read_number(V, file);
            return Self{
                .key_size = key_size,
                .key = key_value,
                .value = value,
            };
        }

        pub inline fn read_from_offset(allocator: Allocator, file: File, offset: u32) !Self {
            try file.seekTo(@intCast(offset));
            const result = try read(allocator, file);
            return result;
        }
    };
}
