const std = @import("std");
const data_types = @import("./data_types.zig");
const LinkRecord = data_types.LinkRecord;

const DEFAULT_LINK_STORE_FILE = "./links.store";

const LinkStorage = struct {
    path: []const u8 = (&DEFAULT_LINK_STORE_FILE).*,
    file: ?std.fs.File = null,

    pub fn open(self: *LinkStorage) !void {
        self.file = try std.fs.cwd().createFile(self.path, .{
            .read = true,
            .truncate = false,
        });
    }

    pub inline fn close(self: LinkStorage) void {
        self.file.?.close();
        self.file = null;
    }

    pub fn write(self: LinkStorage, record: LinkRecord) !void {
        try self.file.?.seekFromEnd(0);
        try self.writeItem(@TypeOf(record.src_node_id), record.src_node_id);
        try self.writeItem(@TypeOf(record.dst_node_id), record.dst_node_id);
        try self.writeItem(@TypeOf(record.src_node_prev_link_ptr), record.src_node_prev_link_ptr);
        try self.writeItem(@TypeOf(record.src_node_next_link_ptr), record.src_node_next_link_ptr);
        try self.writeItem(@TypeOf(record.dst_node_prev_link_ptr), record.dst_node_prev_link_ptr);
        try self.writeItem(@TypeOf(record.dst_node_next_link_ptr), record.dst_node_next_link_ptr);
        if (record.link_label) |label| {
            try self.writeItem(@TypeOf(label), label);
        }
    }

    inline fn writeItem(self: LinkStorage, comptime T: type, item: T) !void {
        var buffer: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buffer, item, std.builtin.Endian.big);
        try self.file.?.writeAll(&buffer);
    }
};
