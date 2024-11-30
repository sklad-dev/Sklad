const std = @import("std");
const data_types = @import("./data_types.zig");

const SEED: u64 = 0;

pub const BloomFilter = struct {
    allocator: std.mem.Allocator,
    filter: []u8,

    pub fn may_contain(self: BloomFilter, key: []const u8) bool {
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

    pub fn init(num_records: u32, bits_per_key: u8, allocator: std.mem.Allocator) !BloomFilter {
        const float_bpk: f32 = @floatFromInt(bits_per_key);
        var num_hashes: u8 = @intFromFloat(float_bpk * 0.69); // 0.69 is approximately ln(2)
        num_hashes = @min(30, @max(1, num_hashes));

        var filter_size_bits: u32 = @max(64, num_records * bits_per_key);
        const filter_size_bytes: u32 = ((filter_size_bits + 7) / 8);
        filter_size_bits = filter_size_bytes * 8;

        const buf = try allocator.alloc(u8, filter_size_bytes + 1);
        @memset(buf, 0);
        buf[buf.len - 1] = num_hashes;

        return BloomFilter{
            .allocator = allocator,
            .filter = buf,
        };
    }

    pub fn add(self: BloomFilter, key: []const u8) void {
        const num_hashes = self.filter[self.filter.len - 1];
        const filter_size_bits: u32 = (@as(u32, @intCast(self.filter.len)) - 1) * 8;
        var hash = std.hash.XxHash32.hash(SEED, key);
        const delta = (hash >> 17) | (hash << 15);
        for (0..num_hashes) |_| {
            const position = hash % filter_size_bits;
            self.filter[position / 8] |= @as(u8, 1) << @intCast((position % 8));
            hash = (hash & delta) << 1;
        }
    }

    pub inline fn deinit(self: *BloomFilter) void {
        self.allocator.free(self.filter);
    }
};

// Tests
const testing = std.testing;
const ValueType = data_types.ValueType;
const utils = @import("./utils.zig");

test "test" {
    const val1: u64 = 1;
    const val2: u64 = 2;
    const val3: u64 = 3;

    var filter = try BloomFilter.init(2, 10, testing.allocator);
    defer filter.deinit();

    filter.add(&utils.key_from_int_data(u64, val1));
    filter.add(&utils.key_from_int_data(u64, val2));

    try testing.expect(filter.may_contain(&utils.key_from_int_data(u64, val1)) == true);
    try testing.expect(filter.may_contain(&utils.key_from_int_data(u64, val3)) == false);
}
