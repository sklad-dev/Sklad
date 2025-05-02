const std = @import("std");
const data_types = @import("./data_types.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const tryLockFor = @import("./utils.zig").tryLockFor;

const StorageRecord = data_types.StorageRecord;

const DEFAULT_WAL_FILE = "./.wal";

pub const Wal = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    lock: std.Thread.Mutex = .{},

    pub fn open(allocator: std.mem.Allocator, wal_file_path: []const u8) !Wal {
        return .{
            .allocator = allocator,
            .path = wal_file_path,
            .file = try std.fs.cwd().createFile(wal_file_path, .{
                .read = true,
                .truncate = false,
            }),
        };
    }

    pub fn closeAndFree(self: *const Wal) void {
        self.file.close();
        self.allocator.free(self.path);
    }

    pub inline fn isEmpty(self: *const Wal) !bool {
        const info = try self.file.stat();
        return info.size == 0;
    }

    pub fn write(self: *Wal, record: *const StorageRecord) !void {
        if (!tryLockFor(&self.lock, 200)) return ApplicationError.ExecutionTimeout;
        defer self.lock.unlock();

        try self.file.seekFromEnd(0);
        try record.write(self.file);
    }

    pub fn deleteFile(self: *const Wal) !void {
        std.fs.cwd().deleteFile(self.path) catch {
            const out = std.io.getStdOut().writer();
            try std.fmt.format(out, "failed to delete wal file {s}\n", .{self.path});
        };
    }

    pub fn readRecord(self: *const Wal, allocator: std.mem.Allocator) !StorageRecord {
        const record = try StorageRecord.read(allocator, self.file);
        return record;
    }

    pub fn name(self: *const Wal) []const u8 {
        return self.path[(self.path.len - 8)..];
    }
};
