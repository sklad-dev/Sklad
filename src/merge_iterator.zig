const std = @import("std");
const utils = @import("utils.zig");
const StorageRecord = @import("data_types.zig").StorageRecord;

pub const MergeIterator = struct {
    const Self = @This();

    const Source = struct {
        iterator: StorageRecord.Iterator,
        current: ?StorageRecord,
    };

    allocator: std.mem.Allocator,
    tree: []i8,
    sources: []Source,

    pub fn init(allocator: std.mem.Allocator, iterators: []StorageRecord.Iterator) !Self {
        // TODO: assert sources.len < 128
        const tree = try allocator.alloc(i8, iterators.len);
        @memset(tree, -1);

        var sources = try allocator.alloc(Source, iterators.len);
        for (iterators, 0..) |it, i| {
            sources[i] = .{
                .iterator = it,
                .current = null,
            };
        }

        var self = Self{
            .allocator = allocator,
            .tree = tree,
            .sources = sources,
        };
        try self.buildInitialTree();
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.tree);
        self.allocator.free(self.sources);
    }

    pub fn next(self: *Self) !?StorageRecord {
        if (self.tree[self.tree.len - 1] < 0) return null;

        const winner_index: usize = @intCast(self.tree[self.tree.len - 1]);
        const result = self.sources[winner_index].current;

        self.sources[winner_index].current = try self.sources[winner_index].iterator.next();
        self.replay(@intCast(winner_index));
        return result;
    }

    fn buildInitialTree(self: *Self) !void {
        for (self.sources) |*source| {
            source.current = try source.iterator.next();
        }

        for (0..self.sources.len) |i| {
            self.replay(@intCast(i));
        }
    }

    fn replay(self: *Self, source_index: i8) void {
        if (source_index < 0) return;

        var current_winner = source_index;
        var tree_index: usize = @as(usize, @intCast(source_index)) / 2;

        while (tree_index < self.tree.len - 1) {
            const loser = self.tree[tree_index];
            if (loser >= 0) {
                const match_winner = self.playMatch(current_winner, loser);

                if (match_winner == current_winner) {
                    self.tree[tree_index] = loser;
                } else {
                    self.tree[tree_index] = current_winner;
                    current_winner = loser;
                }
            } else {
                self.tree[tree_index] = current_winner;
                return;
            }
            tree_index = (tree_index + self.sources.len) / 2;
        }

        const root_index = self.tree.len - 1;
        const champion = self.tree[root_index];

        if (champion >= 0) {
            const match_winner = self.playMatch(current_winner, champion);
            if (match_winner == current_winner) {
                self.tree[root_index] = current_winner;
            }
        } else {
            self.tree[root_index] = current_winner;
        }
    }

    fn playMatch(self: *Self, idx1: i8, idx2: i8) i8 {
        const v1 = self.sources[@intCast(idx1)].current;
        const v2 = self.sources[@intCast(idx2)].current;

        if (v1 == null and v2 == null) return idx1;
        if (v1 == null) return idx2;
        if (v2 == null) return idx1;

        const result = utils.compareBitwise(v1.?.key, v2.?.key);
        return if (result <= 0) idx1 else idx2;
    }
};

// Tests
const testing = std.testing;

fn TestIterator(comptime size: usize) type {
    return struct {
        const Self = @This();

        buf: [size * 2]u8,
        index: usize = 0,

        pub fn init(data: [size]i8) Self {
            var self: Self = .{ .buf = undefined, .index = 0 };

            const record_size: usize = 2;
            var i: usize = 0;
            while (i < size) : (i += 1) {
                const v = data[i];

                const key_bytes = utils.intToBytes(i8, v);
                const value_bytes = utils.intToBytes(i8, v);

                const offset = i * record_size;
                self.buf[offset + 0] = key_bytes[0];
                self.buf[offset + 1] = value_bytes[0];
            }

            return self;
        }

        pub fn next(ptr: *anyopaque) !?StorageRecord {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.index >= size) return null;

            const offset = self.index * 2;
            self.index += 1;

            const key_slice = self.buf[offset .. offset + 1];
            const value_slice = self.buf[offset + 1 .. offset + 2];

            return StorageRecord{
                .key = key_slice,
                .value = value_slice,
            };
        }

        pub fn iterator(self: *Self) StorageRecord.Iterator {
            return .{
                .context = self,
                .next_fn = next,
            };
        }
    };
}

test "LoserTreeIterator#init" {
    const allocator = std.testing.allocator;
    var iter1 = TestIterator(2).init([2]i8{ 1, 9 });
    var iter2 = TestIterator(2).init([2]i8{ 2, 10 });
    var iter3 = TestIterator(2).init([2]i8{ 3, 11 });
    var iter4 = TestIterator(2).init([2]i8{ 4, 12 });
    var iter5 = TestIterator(2).init([2]i8{ 5, 13 });
    var iter6 = TestIterator(2).init([2]i8{ 6, 14 });
    var iter7 = TestIterator(2).init([2]i8{ 7, 15 });

    var source_iters: [7]StorageRecord.Iterator = .{
        iter1.iterator(),
        iter2.iterator(),
        iter3.iterator(),
        iter4.iterator(),
        iter5.iterator(),
        iter6.iterator(),
        iter7.iterator(),
    };

    var loser_tree_iter = try MergeIterator.init(allocator, &source_iters);
    defer loser_tree_iter.deinit();

    const expected_tree: [7]i8 = .{ 1, 3, 5, 6, 4, 2, 0 };
    try std.testing.expect(std.mem.eql(i8, loser_tree_iter.tree, &expected_tree));
}

test "LoserTreeIterator#next" {
    const allocator = std.testing.allocator;
    var iter1 = TestIterator(4).init([4]i8{ 1, 5, 9, 13 });
    var iter2 = TestIterator(4).init([4]i8{ 2, 6, 10, 14 });
    var iter3 = TestIterator(4).init([4]i8{ 3, 7, 11, 15 });
    var iter4 = TestIterator(4).init([4]i8{ 4, 8, 12, 16 });

    var source_iters: [4]StorageRecord.Iterator = .{
        iter1.iterator(),
        iter2.iterator(),
        iter3.iterator(),
        iter4.iterator(),
    };

    var loser_tree_iter = try MergeIterator.init(allocator, &source_iters);
    defer loser_tree_iter.deinit();

    for (0..16) |i| {
        const rec = try loser_tree_iter.next();
        try testing.expect(rec != null);

        const expected: u8 = @intCast(i + 1);
        try testing.expect(std.mem.eql(u8, rec.?.key, &utils.intToBytes(u8, expected)));
    }
}
