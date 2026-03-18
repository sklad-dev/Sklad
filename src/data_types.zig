const std = @import("std");
const utils = @import("./utils.zig");
const File = std.fs.File;
const Allocator = std.mem.Allocator;

pub const EMPTY_VALUE: []u8 = &[_]u8{};
pub const FLAG_TTL: u8 = 0b00000001;

pub const FileHandle = struct {
    level: u8,
    file_id: u64,
};

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

pub const KeyValuePair = struct {
    key: std.json.Value,
    value: std.json.Value,
};

pub const BinaryData = []const u8;

pub const BinaryDataRange = struct {
    start: BinaryData,
    end: BinaryData,
};

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

    pub inline fn toBytes(self: *const TypedBinaryData, allocator: std.mem.Allocator) ![]u8 {
        const buffer = try allocator.alloc(u8, 1 + self.data.len);
        buffer[0] = @intFromEnum(self.data_type);
        @memcpy(buffer[1..], self.data);
        return buffer;
    }
};

pub const RecordKey = struct {
    data: BinaryData,
    timestamp: i64,
};

pub const RecordValueFlags = ?u8;
inline fn isTtlFlagSet(flags: RecordValueFlags) bool {
    return (flags orelse 0) & FLAG_TTL != 0;
}

pub const RecordValue = struct {
    data: BinaryData,
    flags: RecordValueFlags = null,
    ttl: ?i64 = null,

    pub inline fn isTombstone(self: *const RecordValue) bool {
        return self.data.len == 0;
    }

    pub inline fn hasTtl(self: *const RecordValue) bool {
        return isTtlFlagSet(self.flags);
    }

    pub inline fn sizeInMemory(self: *const RecordValue) usize {
        if (self.isTombstone()) return 0;
        return StorageRecord.FLAGS_BYTES + self.data.len + if (self.hasTtl()) StorageRecord.TTL_BYTES else 0;
    }

    pub inline fn tombstone() RecordValue {
        return .{ .data = EMPTY_VALUE, .flags = null, .ttl = null };
    }
};

pub const StorageRecord = struct {
    key: RecordKey,
    value: RecordValue,

    pub const DATA_SIZE_BYTES: usize = 2;
    pub const TIMESTAMP_BYTES: usize = 8;
    pub const FLAGS_BYTES: usize = 1;
    pub const TTL_BYTES: usize = 8;
    pub const KEY_HEADER_BYTES: usize = DATA_SIZE_BYTES + TIMESTAMP_BYTES;
    pub const HEADER_BYTES: usize = DATA_SIZE_BYTES * 2 + TIMESTAMP_BYTES;
    pub const HEADER_FLAGS_BYTES: usize = DATA_SIZE_BYTES * 2 + TIMESTAMP_BYTES + FLAGS_BYTES;

    pub const Iterator = struct {
        context: *anyopaque,
        next_fn: *const fn (ctx: *anyopaque) anyerror!?StorageRecord,

        pub fn next(self: *Iterator) anyerror!?StorageRecord {
            return self.next_fn(self.context);
        }
    };

    pub fn init(key: BinaryData, value: BinaryData, timestamp: i64, ttl: ?i64) StorageRecord {
        return .{
            .key = .{ .data = key, .timestamp = timestamp },
            .value = .{
                .data = value,
                .flags = if (value.len == 0) null else if (ttl) |_| FLAG_TTL else 0,
                .ttl = ttl,
            },
        };
    }

    pub inline fn fromBytes(buffer: []const u8, offset: usize) StorageRecord {
        const key_size: u16 = utils.intFromBytes(u16, buffer, offset);
        const timestamp = utils.intFromBytes(i64, buffer, offset + DATA_SIZE_BYTES + key_size);
        const value_size: u16 = utils.intFromBytes(u16, buffer, offset + KEY_HEADER_BYTES + key_size);
        const flags: ?u8 = if (value_size > 0) buffer[offset + key_size + HEADER_BYTES] else null;

        return .{
            .key = .{
                .data = buffer[offset + DATA_SIZE_BYTES .. offset + DATA_SIZE_BYTES + key_size],
                .timestamp = timestamp,
            },
            .value = .{
                .data = if (value_size > 0) buffer[offset + key_size + HEADER_FLAGS_BYTES .. offset + key_size + HEADER_FLAGS_BYTES + value_size] else EMPTY_VALUE,
                .flags = flags,
                .ttl = if (isTtlFlagSet(flags)) utils.intFromBytes(i64, buffer, offset + key_size + HEADER_FLAGS_BYTES + value_size) else null,
            },
        };
    }

    pub inline fn isTombstone(self: *const StorageRecord) bool {
        return self.value.isTombstone();
    }

    pub inline fn hasTtl(self: *const StorageRecord) bool {
        return self.value.hasTtl();
    }

    pub inline fn isExpired(self: *const StorageRecord) bool {
        if (self.value.ttl) |ttl| {
            return std.time.milliTimestamp() > @divTrunc(self.key.timestamp, std.time.us_per_ms) + ttl;
        }
        return false;
    }

    pub inline fn sizeInMemory(self: *const StorageRecord) usize {
        return HEADER_BYTES + self.key.data.len + self.value.sizeInMemory();
    }

    pub fn write(self: *const StorageRecord, writer: *std.fs.File.Writer) !void {
        try utils.writeStorageKey(writer, &self.key);
        try utils.writeNumber(i64, &writer.interface, self.key.timestamp);
        if (!self.isTombstone()) {
            try utils.writeStorageValue(writer, &self.value);
        } else {
            try utils.writeNumber(u16, &writer.interface, 0);
        }
    }

    pub fn writeToBuffer(self: *const StorageRecord, buffer: []u8, offset: usize) void {
        var pos: usize = offset;

        buffer[pos..][0..DATA_SIZE_BYTES].* = utils.intToBytes(u16, @intCast(self.key.data.len));
        pos += DATA_SIZE_BYTES;

        @memcpy(buffer[pos..][0..self.key.data.len], self.key.data);
        pos += self.key.data.len;

        buffer[pos..][0..TIMESTAMP_BYTES].* = utils.intToBytes(i64, self.key.timestamp);
        pos += TIMESTAMP_BYTES;

        buffer[pos..][0..DATA_SIZE_BYTES].* = utils.intToBytes(u16, @intCast(self.value.data.len));
        pos += DATA_SIZE_BYTES;

        if (!self.isTombstone()) {
            buffer[pos] = self.value.flags.?;
            pos += FLAGS_BYTES;

            @memcpy(buffer[pos..][0..self.value.data.len], self.value.data);
            pos += self.value.data.len;

            if (self.value.hasTtl()) {
                buffer[pos..][0..TTL_BYTES].* = utils.intToBytes(i64, self.value.ttl.?);
                pos += TTL_BYTES;
            }
        }
    }

    pub fn destroy(self: *const StorageRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.key.data);
        if (!self.isTombstone()) {
            allocator.free(self.value.data);
        }
    }

    pub fn read(allocator: Allocator, reader: *std.fs.File.Reader, offset: u32) !StorageRecord {
        try reader.seekTo(offset);
        const key_size: u16 = try utils.readNumber(u16, &reader.interface);
        try reader.seekTo(offset + DATA_SIZE_BYTES);
        const key: []u8 = try allocator.alloc(u8, key_size);
        _ = try reader.interface.readSliceAll(key[0..]);

        try reader.seekTo(offset + DATA_SIZE_BYTES + key_size);
        const timestamp: i64 = try utils.readNumber(i64, &reader.interface);

        try reader.seekTo(offset + KEY_HEADER_BYTES + key_size);
        const value_size: u16 = try utils.readNumber(u16, &reader.interface);

        if (value_size == 0) {
            return .{
                .key = .{ .data = key, .timestamp = timestamp },
                .value = RecordValue.tombstone(),
            };
        }

        try reader.seekTo(offset + key_size + HEADER_BYTES);
        const flags = try utils.readNumber(u8, &reader.interface);

        try reader.seekTo(offset + key_size + HEADER_FLAGS_BYTES);
        const value = try allocator.alloc(u8, value_size);
        errdefer allocator.free(value);
        _ = try reader.interface.readSliceAll(value[0..]);

        var ttl: ?i64 = null;
        if (isTtlFlagSet(flags)) {
            try reader.seekTo(offset + key_size + HEADER_FLAGS_BYTES + value_size);
            ttl = try utils.readNumber(i64, &reader.interface);
        }

        return .{
            .key = .{ .data = key, .timestamp = timestamp },
            .value = .{ .data = value, .flags = flags, .ttl = ttl },
        };
    }
};
