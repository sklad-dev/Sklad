const std = @import("std");
const IO = @import("./io.zig").IO;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }
    var io = try IO.init(gpa.allocator());
    defer io.deinit();

    io.listen();
}
