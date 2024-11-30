const std = @import("std");
const data_types = @import("./data_types.zig");
const ValueType = data_types.ValueType;
const StorageRecord = data_types.StorageRecord;

const DEFAULT_WAL_FILE = "./.wal";

pub fn Wal(comptime V: type) type {
    return struct {
        path: []const u8 = (&DEFAULT_WAL_FILE).*,
        file: ?std.fs.File = null,

        const Self = @This();

        pub fn open(self: *Self) !void {
            self.file = try std.fs.cwd().createFile(self.path, .{
                .read = true,
                .truncate = false,
            });
        }

        pub fn close(self: *Self) void {
            if (self.file) |file| {
                file.close();
                self.file = null;
            }
        }

        pub inline fn is_empty(self: Self) !bool {
            if (self.file) |file| {
                const info = try file.stat();
                return info.size == 0;
            }
            return false;
        }

        pub fn write(self: Self, record: *const StorageRecord(V)) !void {
            try self.file.?.seekFromEnd(0);
            try record.write(self.file.?);
        }

        pub fn delete_file(self: Self) !void {
            std.fs.cwd().deleteFile(self.path) catch {
                const out = std.io.getStdOut().writer();
                try std.fmt.format(out, "failed to clean up after the test\n", .{});
            };
        }

        pub fn read_record(self: Self, allocator: std.mem.Allocator) !StorageRecord(V) {
            const record = try StorageRecord(V).read(allocator, self.file.?);
            return record;
        }
    };
}
