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

const NodeRecord = struct {
    first_relationship_pointer: usize,
    value_type: ValueType,
    value_size: u32,
    value: []u8,
};
