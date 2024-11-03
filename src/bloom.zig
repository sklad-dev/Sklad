const std = @import("std");
const data_types = @import("./data_types.zig");
const NodeRecord = data_types.NodeRecord;

const SEED: u64 = 0;

pub const BloomFilter = struct {
    allocator: std.mem.Allocator,
    filter: []u8,

    const Self = @This();

    pub fn may_contain(self: Self, key: []const u8) bool {
        if (self.filter.len < 2) {
            return false;
        }
        const num_hashes = self.filter[self.filter.len - 1];
        if (num_hashes > 30) {
            return true;
        }
        const filter_size_bits: u32 = (@as(u32, @intCast(self.filter.len)) - 1) * 8;

        var hash = std.hash.XxHash32.hash(SEED, key);
        const delta = (hash >> 17) | (hash << 15);
        for (0..num_hashes) |_| {
            const position = hash % filter_size_bits;
            if (self.filter[position / 8] & (@as(u8, 1) << @intCast((position % 8))) == 0) {
                return false;
            }
            hash = (hash & delta) << 1;
        }

        return true;
    }

    pub fn create(records: []*const NodeRecord, bits_per_key: u8, allocator: std.mem.Allocator) !Self {
        const float_bpk: f32 = @floatFromInt(bits_per_key);
        var num_hashes: u8 = @intFromFloat(float_bpk * 0.69); // 0.69 is approximately ln(2)
        num_hashes = @min(30, @max(1, num_hashes));

        var filter_size_bits: u32 = @max(64, @as(u32, @intCast(records.len)) * bits_per_key);
        const filter_size_byts: u32 = ((filter_size_bits + 7) / 8);
        filter_size_bits = filter_size_byts * 8;

        const buf = try allocator.alloc(u8, filter_size_byts + 1);
        @memset(buf, 0);

        for (records) |record| {
            var hash = std.hash.XxHash32.hash(SEED, record.*.value);
            const delta = (hash >> 17) | (hash << 15);
            for (0..num_hashes) |_| {
                const position = hash % filter_size_bits;
                buf[position / 8] |= @as(u8, 1) << @intCast((position % 8));
                hash = (hash & delta) << 1;
            }
        }
        buf[buf.len - 1] = num_hashes;

        return Self{
            .allocator = allocator,
            .filter = buf,
        };
    }

    pub fn delete(self: *Self) void {
        self.allocator.free(self.filter);
    }
};

// Tests
const testing = std.testing;
const ValueType = data_types.ValueType;

pub fn key_from_int_data(comptime T: type, key_value: T) [@sizeOf(T)]u8 {
    var buffer: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buffer, key_value, std.builtin.Endian.big);
    return buffer;
}

test "test" {
    const val1: u64 = 1;
    const record1 = NodeRecord{
        .node_id = 1,
        .value_type = ValueType.bigserial,
        .value_size = @sizeOf(u64),
        .value = &key_from_int_data(u64, val1),
    };

    const val2: u64 = 2;
    const record2 = NodeRecord{
        .node_id = 1,
        .value_type = ValueType.bigserial,
        .value_size = @sizeOf(u64),
        .value = &key_from_int_data(u64, val2),
    };

    const records = try testing.allocator.alloc(*const NodeRecord, 2);
    defer testing.allocator.free(records);
    records[0] = &record1;
    records[1] = &record2;

    const val3: u64 = 3;

    var filter = try BloomFilter.create(records, 10, testing.allocator);
    defer filter.delete();

    try testing.expect(filter.may_contain(&key_from_int_data(u64, val1)) == true);
    try testing.expect(filter.may_contain(&key_from_int_data(u64, val3)) == false);
}
