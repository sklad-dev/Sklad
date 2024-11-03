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
};

pub const NodePointer = u64;

// A pointer to a node record stored on disk
pub const FsNodePointer = struct {
    padding: u8,
    level_id: u8,
    file_id: u8,
    offset: u32,
};

pub const NodeRecord = struct {
    node_id: u64,
    value_type: ValueType,
    value_size: u16,
    value: []const u8,
};

pub const LinkRecord = struct {
    src_node_id: u64,
    dst_node_id: u64,
    link_label: ?u64,
};
