const std = @import("std");
const utils = @import("./utils.zig");
const ValueType = @import("./data_types.zig").ValueType;
const TypedStorage = @import("./typed_storage.zig").TypedStorage;

pub const QueryError = error{
    UnknownOperation,
};

pub const Operations = enum(u8) {
    INSERT = 0,
    CONNECT,
    FIND,
};

pub fn exec(storage: *TypedStorage, query_buffer: []u8) !u64 {
    return switch (query_buffer[0]) {
        0 => insert_blk: {
            const data_size: u16 = utils.int_from_bytes(u16, query_buffer, 1);
            const data_type: ValueType = @enumFromInt(utils.int_from_bytes(u8, query_buffer, 3));
            const data: []u8 = query_buffer[4 .. 4 + data_size];
            const node_id = try storage.node_storage.put(data, data_type);
            break :insert_blk node_id;
        },
        1 => 1,
        2 => find_blk: {
            const data_size: u16 = utils.int_from_bytes(u16, query_buffer, 1);
            const data_type: ValueType = @enumFromInt(utils.int_from_bytes(u8, query_buffer, 3));
            const data: []u8 = query_buffer[4 .. 4 + data_size];
            const node_id = try storage.node_storage.find(data, data_type);
            break :find_blk node_id orelse 0xFFFFFFFFFFFFFFFF;
        },
        else => QueryError.UnknownOperation,
    };
}
