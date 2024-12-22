const std = @import("std");
const IO = @import("./io.zig").IO;

pub fn main() !void {
    const io = try IO.init();
    defer io.deinit();

    io.listen();
}
