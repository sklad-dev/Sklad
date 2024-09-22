const std = @import("std");
const data_types = @import("./data_types.zig");

pub const MemtableKey = []const u8;

pub const MemtableValue = struct {
    first_relationship_pointer: usize,
    value_type: data_types.ValueType,
    value_size: u32,
};

pub fn keyFromIntData(comptime T: type, key_value: T) [@sizeOf(T)]u8 {
    var buffer: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buffer, key_value, std.builtin.Endian.big);
    return buffer;
}

pub fn Memtable(comptime T: type) type {
    return struct {
        const Self = @This();

        addFn: *const fn (*T, MemtableKey, MemtableValue) anyerror!void,
        findFn: *const fn (T, MemtableKey) ?MemtableValue,

        pub fn implement() Self {
            return Self{
                .addFn = T.add,
                .findFn = T.find,
            };
        }

        pub fn add(self: *Memtable, key: MemtableKey, value: MemtableValue) !void {
            self.addFn(key, value);
        }

        pub fn find(self: Memtable, key: MemtableKey) ?MemtableValue {
            return self.findFn(key);
        }
    };
}
