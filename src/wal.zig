const std = @import("std");
const data_types = @import("./data_types.zig");

const StorageRecord = data_types.StorageRecord;

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

    pub inline fn is_empty(self: *Wal) !bool {
        if (self.file) |file| {
            const info = try file.stat();
            return info.size == 0;
        }
        return false;
    }

    pub fn write(self: *Wal, record: *const StorageRecord) !void {
        try self.file.?.seekFromEnd(0);
        try record.write(self.file.?);
    }

    pub fn delete_file(self: *const Wal) !void {
        std.fs.cwd().deleteFile(self.path) catch {
            const out = std.io.getStdOut().writer();
            try std.fmt.format(out, "failed to clean up after the test\n", .{});
        };
    }

    pub fn read_record(self: *Wal, allocator: std.mem.Allocator) !StorageRecord {
        const record = try StorageRecord.read(allocator, self.file.?);
        return record;
    }
};
