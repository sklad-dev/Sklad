const std = @import("std");
const data_types = @import("./data_types.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const tryLockFor = @import("./utils.zig").tryLockFor;
const getWorkerContext = @import("./worker.zig").getWorkerContext;

const StorageRecord = data_types.StorageRecord;

const DEFAULT_WAL_FILE = "./.wal";

pub const WalError = error{
    SyncronizationError,
};

pub const Wal = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    eof_offset: std.atomic.Value(u64) align(std.atomic.cache_line),
    synced_offset: std.atomic.Value(u64) align(std.atomic.cache_line),
    written_offset: std.atomic.Value(u64) align(std.atomic.cache_line),
    is_flushing: std.atomic.Value(bool) align(std.atomic.cache_line) = std.atomic.Value(bool).init(false),
    sync_mutex: std.Thread.Mutex align(std.atomic.cache_line) = .{},
    sync_cond: std.Thread.Condition align(std.atomic.cache_line) = .{},

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
            .written_offset = std.atomic.Value(u64).init(eof_offset),
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

        const offset = self.eof_offset.fetchAdd(record_size, .acq_rel);
        const end_offset = offset + record_size;
        self.file.pwriteAll(slice, offset) catch |e| {
            _ = self.written_offset.rmw(.Max, end_offset, .seq_cst);
            return e;
        };

        while (self.written_offset.load(.acquire) < offset) {
            std.Thread.yield() catch {};
        }
        _ = self.written_offset.rmw(.Max, end_offset, .seq_cst);

        self.sync_mutex.lock();
        while (self.synced_offset.load(.acquire) < end_offset) {
            if (!self.is_flushing.swap(true, .acq_rel)) {
                const ready_to_sync = self.written_offset.load(.acquire);
                self.sync_mutex.unlock();

                self.file.sync() catch |e| {
                    self.sync_mutex.lock();
                    self.is_flushing.store(false, .release);
                    self.sync_cond.broadcast();
                    self.sync_mutex.unlock();
                    std.log.err("Error! Wal syncronization failed: {any}", .{e});
                    return WalError.SyncronizationError;
                };

                self.sync_mutex.lock();
                self.synced_offset.store(ready_to_sync, .release);
                self.is_flushing.store(false, .release);
                self.sync_cond.broadcast();
            } else {
                self.sync_cond.wait(&self.sync_mutex);
            }
        }
        self.sync_mutex.unlock();
    }

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
