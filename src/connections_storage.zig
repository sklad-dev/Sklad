const std = @import("std");
const data_types = @import("./data_types.zig");
const utils = @import("./utils.zig");

const Storage = @import("./storage.zig").Storage;

pub const ConnectionRecord = struct {
    src_node_id: u64,
    dst_node_id: u64,
    label: ?u64,
    deleted: bool,
};

pub const ConnectionStorage = struct {
    allocator: std.mem.Allocator,
    storage: Storage(u8),

    pub fn init(allocator: std.mem.Allocator, path: []const u8, max_memtable_size: u16) !ConnectionStorage {
        return ConnectionStorage{
            .allocator = allocator,
            .storage = try Storage(u8).start(allocator, path, max_memtable_size),
        };
    }

    pub inline fn stop(self: *ConnectionStorage) void {
        self.storage.stop();
    }

    pub fn put(self: *ConnectionStorage, src_id: u64, dst_id: u64, label_id: u64) !void {
        const storage_key = try self.build_key(src_id, dst_id, label_id);
        defer self.allocator.free(storage_key);

        try self.storage.put(storage_key, 1);
    }

    pub fn delete(self: *ConnectionStorage, src_id: u64, dst_id: u64, label_id: u64) !void {
        const storage_key = try self.build_key(src_id, dst_id, label_id);
        defer self.allocator.free(storage_key);

        try self.storage.put(storage_key, 0);
    }

    pub fn find(self: *ConnectionStorage, src_id: u64, dst_id: u64, label_id: u64) !?u8 {
        const storage_key = try self.build_key(src_id, dst_id, label_id);
        defer self.allocator.free(storage_key);

        const result = try self.storage.find(storage_key);
        if (result == 0) {
            return null;
        } else {
            return result;
        }
    }

    inline fn build_key(self: *const ConnectionStorage, src_id: u64, dst_id: u64, label_id: u64) ![]u8 {
        const key_buffer = try self.allocator.alloc(u8, 24);
        write_to_key(key_buffer, src_id, 0);
        write_to_key(key_buffer, dst_id, 8);
        write_to_key(key_buffer, label_id, 16);
        return key_buffer;
    }

    inline fn write_to_key(key_buffer: []u8, value: u64, offset: u8) void {
        var value_buffer: [8]u8 = undefined;
        std.mem.writeInt(u64, &value_buffer, value, std.builtin.Endian.big);
        @memcpy(key_buffer[offset .. offset + 8], &value_buffer);
    }
};

// Tests
const testing = std.testing;

fn clean_up(node_storage: *ConnectionStorage) void {
    for (node_storage.storage.memtables.items) |t| {
        t.wal.delete_file() catch {
            const out = std.io.getStdOut().writer();
            std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
        };
    }
    var it = node_storage.storage.table_file_manager.files.iterator();
    while (it.next()) |entry| {
        for (entry.value_ptr.*.items) |file_name| {
            std.fs.cwd().deleteFile(file_name) catch {
                const out = std.io.getStdOut().writer();
                std.fmt.format(out, "failed to clean up after the test\n", .{}) catch unreachable;
            };
        }
    }
}

test "ConnectionStorage" {
    var node_storage = try ConnectionStorage.init(testing.allocator, "./", 4);
    defer node_storage.stop();
    defer clean_up(&node_storage);

    try node_storage.put(1, 2, 1);
    try testing.expect(try node_storage.find(1, 2, 1) == 1);
    try testing.expect(try node_storage.find(1, 3, 1) == null);
    try testing.expect(try node_storage.find(2, 2, 1) == null);
    try testing.expect(try node_storage.find(1, 2, 2) == null);

    try node_storage.delete(1, 2, 1);
    try testing.expect(try node_storage.find(1, 2, 1) == null);

    for (3..8) |i| {
        try node_storage.put(1, i, 1);
    }
    try testing.expect(try node_storage.find(1, 4, 1) == 1);
    for (3..8) |i| {
        try node_storage.delete(1, i, 1);
    }
    try testing.expect(try node_storage.find(1, 4, 1) == null);
}
