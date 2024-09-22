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
