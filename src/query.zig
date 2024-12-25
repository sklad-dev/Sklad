const std = @import("std");
const ValueType = @import("./data_types.zig").ValueType;
const GraphStorage = @import("./graph_storage.zig").GraphStorage;

pub const QueryError = error{
    UnknownOperation,
};

pub const Operations = enum(u8) {
    INSERT = 0,
    CONNECT,
    FIND,
};

pub const Node = struct {
    value_size: u16,
    value_type: ValueType,
    value: []const u8,
};

pub fn exec(graph_storage: *GraphStorage, query_buffer: []u8) !u64 {
    return switch (query_buffer[0]) {
        0 => insert_blk: {
            const data_size: u16 = read_number(u16, query_buffer, 1);
            const data_type: ValueType = @enumFromInt(read_number(u8, query_buffer, 3));
            const data: []u8 = query_buffer[4 .. 4 + data_size];
            try graph_storage.node_storage.put(data, data_type);
            break :insert_blk 1;
        },
        1 => 1,
        2 => find_blk: {
            const data_size: u16 = read_number(u16, query_buffer, 1);
            const data_type: ValueType = @enumFromInt(read_number(u8, query_buffer, 3));
            const data: []u8 = query_buffer[4 .. 4 + data_size];
            const node_id = try graph_storage.node_storage.find(data, data_type);
            break :find_blk node_id orelse 0xFFFFFFFFFFFFFFFF;
        },
        else => QueryError.UnknownOperation,
    };
}

inline fn read_number(comptime T: type, buffer: []u8, offset: usize) T {
    return std.mem.readInt(T, buffer[offset .. offset + @sizeOf(T)], std.builtin.Endian.big);
}
