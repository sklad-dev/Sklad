const std = @import("std");

pub fn LoserTreeIterator(comptime T: type) type {
    return struct {
        const Self = @This();

        pub const SourceIterator = struct {
            context: *anyopaque,
            next_fn: *const fn (ptr: *anyopaque) ?T,
            current: ?T,

            pub fn next(self: *SourceIterator) ?T {
                self.current = self.next_fn(self.context);
                return self.current;
            }
        };

        allocator: std.mem.Allocator,
        tree: []i8,
        sources: []SourceIterator,
        compare_fn: *const fn (T, T) i8,

        pub fn init(allocator: std.mem.Allocator, sources: []SourceIterator, compare_fn: *const fn (T, T) i8) !Self {
            // TODO: assert sources.len < 128
            const tree = try allocator.alloc(i8, sources.len);
            @memset(tree, -1);

            var self = Self{
                .allocator = allocator,
                .tree = tree,
                .sources = sources,
                .compare_fn = compare_fn,
            };
            self.buildInitialTree();
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.tree);
        }

        pub fn next(self: *Self) ?T {
            if (self.tree[self.tree.len - 1] < 0) return null;

            const winner_index: usize = @intCast(self.tree[self.tree.len - 1]);
            const result = self.sources[winner_index].current;

            _ = self.sources[winner_index].next();
            self.replay(@intCast(winner_index));
            return result;
        }

        fn buildInitialTree(self: *Self) void {
            for (self.sources) |*source| {
                _ = source.next();
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

            const result = self.compare_fn(v1.?, v2.?);
            return if (result <= 0) idx1 else idx2;
        }
    };
}

// Tests
const testing = std.testing;

fn TestIterator(comptime size: usize) type {
    return struct {
        const Self = @This();

        data: [size]i8,
        index: usize,

        pub fn init(data: [size]i8) Self {
            return .{
                .data = data,
                .index = 0,
            };
        }

        pub fn next(ptr: *anyopaque) ?i8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.index >= self.data.len) {
                return null;
            }
            const value = self.data[self.index];
            self.index += 1;
            return value;
        }

        pub fn iterator(self: *Self) LoserTreeIterator(i8).SourceIterator {
            return .{
                .context = self,
                .next_fn = next,
                .current = null,
            };
        }
    };
}

fn testCompare(a: i8, b: i8) i8 {
    return a - b;
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

    var source_iters: [7]LoserTreeIterator(i8).SourceIterator = .{
        iter1.iterator(),
        iter2.iterator(),
        iter3.iterator(),
        iter4.iterator(),
        iter5.iterator(),
        iter6.iterator(),
        iter7.iterator(),
    };

    var loser_tree_iter = try LoserTreeIterator(i8).init(allocator, &source_iters, testCompare);
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

    var source_iters: [4]LoserTreeIterator(i8).SourceIterator = .{
        iter1.iterator(),
        iter2.iterator(),
        iter3.iterator(),
        iter4.iterator(),
    };

    var loser_tree_iter = try LoserTreeIterator(i8).init(allocator, &source_iters, testCompare);
    defer loser_tree_iter.deinit();

    for (0..16) |i| {
        const value = loser_tree_iter.next();
        try std.testing.expect(@as(usize, @intCast(value.?)) == i + 1);
    }
}
