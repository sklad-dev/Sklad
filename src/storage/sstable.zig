const std = @import("std");
const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;

const NodeRecord = struct {
    first_relationship_pointer: usize,
    value_type: ValueType,
    value_size: u32,
    value: []u8,
};

pub const SSTable = struct {
    path: []const u8,
    file: std.fs.File,
};
