pub const ValueType = enum(u8) {
    boolean, //bool
    smallint, // i8
    int, // i32
    bigint, // i64
    tinyserial, // u8
    serial, // u32
    bigserial, // u64
    float, // f32
    bigfloat, //f64
    string, // variable length string
};

pub const NodePointer = u32;

pub const NodeRecord = struct {
    first_relationship_pointer: usize,
    value_type: ValueType,
    value_size: u32,
    value: []u8,
};

pub const LinkRecord = struct {
    src_node_id: u64,
    dst_node_id: u64,
    src_node_prev_link_ptr: u64,
    src_node_next_link_ptr: u64,
    dst_node_prev_link_ptr: u64,
    dst_node_next_link_ptr: u64,
    link_label: ?u64,
};
