const std = @import("std");
const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;
const NodeRecord = data_types.NodeRecord;

const DEFAULT_WAL_FILE = "./.wal";

const Wal = struct {
    path: []const u8 = (&DEFAULT_WAL_FILE).*,
    file: ?std.fs.File = null,

    pub fn open(self: *Wal) !void {
        self.file = try std.fs.cwd().createFile(self.path, .{
            .read = true,
            .truncate = false,
        });
    }

    pub inline fn close(self: Wal) void {
        self.file.?.close();
        self.file = null;
    }

    pub fn write(self: Wal, record: NodeRecord) !void {
        try self.file.?.seekFromEnd(0);
        try self.writeItem(@TypeOf(record.first_relationship_pointer), record.first_relationship_pointer);
        const value_type = @intFromEnum(record.value_type);
        try self.writeItem(@TypeOf(value_type), value_type);
        try self.file.?.writeAll(record.value);
    }

    inline fn writeItem(self: Wal, comptime T: type, item: T) !void {
        var buffer: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buffer, item, std.builtin.Endian.big);
        try self.file.?.writeAll(&buffer);
    }
};
