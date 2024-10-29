const std = @import("std");
const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;
const NodeRecord = data_types.NodeRecord;

const DEFAULT_WAL_FILE = "./.wal";

pub const Wal = struct {
    path: []const u8 = (&DEFAULT_WAL_FILE).*,
    file: ?std.fs.File = null,

    pub fn open(self: *Wal) !void {
        self.file = try std.fs.cwd().createFile(self.path, .{
            .read = true,
            .truncate = false,
        });
    }

    pub fn close(self: *Wal) void {
        if (self.file) |file| {
            file.close();
            self.file = null;
        }
    }

    pub inline fn is_empty(self: Wal) !bool {
        if (self.file) |file| {
            const info = try file.stat();
            return info.size == 0;
        }
        return false;
    }

    pub fn write(self: Wal, record: *const NodeRecord) !void {
        try self.file.?.seekFromEnd(0);
        const value_type = @intFromEnum(record.value_type);
        try self.writeItem(@TypeOf(value_type), value_type);
        try self.writeItem(@TypeOf(record.value_size), record.value_size);
        try self.file.?.writeAll(record.value);
    }

    pub fn delete_file(self: Wal) !void {
        std.fs.cwd().deleteFile(self.path) catch {
            const out = std.io.getStdOut().writer();
            try std.fmt.format(out, "failed to clean up after the test\n", .{});
        };
    }

    inline fn writeItem(self: Wal, comptime T: type, item: T) !void {
        var buffer: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buffer, item, std.builtin.Endian.big);
        try self.file.?.writeAll(&buffer);
    }
};
