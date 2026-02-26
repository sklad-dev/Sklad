const std = @import("std");
const data_types = @import("./data_types.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const tryLockFor = @import("./utils.zig").tryLockFor;
const getWorkerContext = @import("./worker.zig").getWorkerContext;

const StorageRecord = data_types.StorageRecord;

const DEFAULT_WAL_FILE = "./.wal";

pub const Wal = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    eof_offset: std.atomic.Value(u64),
    synced_offset: std.atomic.Value(u64),
    is_flushing: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    sync_mutex: std.Thread.Mutex = .{},
    sync_cond: std.Thread.Condition = .{},

    pub fn open(allocator: std.mem.Allocator, wal_file_path: []const u8) !Wal {
        const file = try std.fs.cwd().createFile(wal_file_path, .{
            .read = true,
            .truncate = false,
        });
        const eof_offset = try file.getEndPos();

        return .{
            .allocator = allocator,
            .path = wal_file_path,
            .file = file,
            .eof_offset = std.atomic.Value(u64).init(eof_offset),
            .synced_offset = std.atomic.Value(u64).init(eof_offset),
        };
    }

    pub fn closeAndFree(self: *const Wal) void {
        self.file.close();
        self.allocator.free(self.path);
    }

    pub inline fn isEmpty(self: *const Wal) !bool {
        return self.eof_offset.load(.acquire) == 0;
    }

    pub fn writeRecord(self: *Wal, record: *const StorageRecord) !void {
        const record_size = record.sizeInMemory();

        var buffer: [1024]u8 = undefined;
        const slice = if (record_size <= 1024) buffer[0..record_size] else try self.allocator.alloc(u8, record_size);
        defer if (record_size > 1024) self.allocator.free(slice);

        record.writeToBuffer(slice, 0);

        const offset = self.eof_offset.fetchAdd(record_size, .seq_cst);
        const my_end_offset = offset + record_size;
        try self.file.pwriteAll(slice, offset);

        self.sync_mutex.lock();
        defer self.sync_mutex.unlock();

        while (self.synced_offset.load(.acquire) < my_end_offset) {
            if (!self.is_flushing.swap(true, .acq_rel)) {
                self.sync_mutex.unlock();

                self.file.sync() catch |err| {
                    self.is_flushing.store(false, .release);
                    return err;
                };

                self.sync_mutex.lock();
                self.synced_offset.store(self.eof_offset.load(.acquire), .release);
                self.is_flushing.store(false, .release);
                self.sync_cond.broadcast();
            } else {
                self.sync_cond.wait(&self.sync_mutex);
            }
        }
    }

    // V2
    // pub fn writeRecord(self: *Wal, record: *const StorageRecord) !void {
    //     const record_size = record.sizeInMemory();
    //     const offset = self.eof_offset.fetchAdd(record_size, .seq_cst);

    //     const buffer = try self.allocator.alloc(u8, record_size);
    //     defer self.allocator.free(buffer);

    //     record.writeToBuffer(buffer, 0);
    //     try self.file.pwriteAll(buffer, offset);

    //     const my_end_offset = offset + record_size;
    //     if (self.synced_offset.load(.acquire) <= my_end_offset) {
    //         if (!self.is_flushing.swap(true, .acq_rel)) {
    //             defer self.is_flushing.store(false, .release);
    //             try self.file.sync();
    //             self.synced_offset.store(self.eof_offset.load(.acquire), .release);
    //         }
    //     }
    // }

    pub fn readRecord(self: *const Wal, allocator: std.mem.Allocator, offset: u32) !StorageRecord {
        var reader = self.file.reader(getWorkerContext().?.reader_buffer[0..]);
        const record = try StorageRecord.read(allocator, &reader, offset);
        return record;
    }

    pub fn deleteFile(self: *const Wal) !void {
        std.fs.cwd().deleteFile(self.path) catch {
            std.log.err("failed to delete wal file {s}\n", .{self.path});
        };
    }

    pub fn name(self: *const Wal) []const u8 {
        return self.path[(self.path.len - 8)..];
    }
};
