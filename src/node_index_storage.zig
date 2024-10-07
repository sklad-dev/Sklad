/// Node index is used to quickly access node value by node id. It is stored on disk as an array of ponters to node records,
/// node id value is represented by an offset of a given pointer in the node index storage file.
/// When the new node is created the corresponding index record is just appended to the end of the file,
/// resulting offset is the new node id.
const std = @import("std");

const DEFAULT_NODE_INDEX_FILE = "./node_index.store";

const NodePointer = u32;

const StorageState = enum {
    open,
    close,
};

const NodeIndexStorage = struct {
    path: []const u8 = (&DEFAULT_NODE_INDEX_FILE).*,
    file: ?std.fs.File = null,
    next_id: u32 = 0,
    state: StorageState = StorageState.close,

    pub fn open(self: *NodeIndexStorage) !void {
        self.file = try std.fs.cwd().createFile(self.path, .{
            .read = true,
            .truncate = false,
        });
        const pos = try self.file.?.getEndPos();
        self.next_id = @intCast(pos / 4);
    }

    pub fn close(self: NodeIndexStorage) void {
        self.file.?.close();
    }

    pub fn allocate_next_id(self: *NodeIndexStorage) !u32 {
        try self.file.?.seekFromEnd(0);
        try self.write(0);
        const current_id = self.next_id;
        self.next_id += 1;
        return current_id;
    }

    pub fn get_record(self: NodeIndexStorage, node_id: u32) !NodePointer {
        try self.file.?.seekTo(@as(u64, node_id) * 4);

        var buffer: [4]u8 = undefined;
        _ = try self.file.?.read(buffer[0..]);

        return std.mem.readInt(u32, &buffer, std.builtin.Endian.big);
    }

    pub fn update_record(self: NodeIndexStorage, node_id: u32, node_pointer: NodePointer) !void {
        try self.file.?.seekTo(@as(u64, node_id) * 4);
        try self.write(node_pointer);
    }

    fn write(self: NodeIndexStorage, node_pointer: NodePointer) !void {
        var buffer: [4]u8 = undefined;
        std.mem.writeInt(u32, &buffer, node_pointer, std.builtin.Endian.big);
        try self.file.?.writeAll(&buffer);
    }
};

// Tests
const testing = std.testing;

fn clean_up(storage: NodeIndexStorage) !void {
    std.fs.cwd().deleteFile(storage.path) catch {
        const out = std.io.getStdOut().writer();
        try std.fmt.format(out, "{s}", .{"failed to clean up after the test\n"});
    };
}

test "NodeIndexStorage#open when there is no storage file" {
    var test_storage: NodeIndexStorage = .{};

    test_storage.open() catch {
        const out = std.io.getStdOut().writer();
        try std.fmt.format(out, "{s}", .{"failed to open a storage file\n"});
    };
    test_storage.close();

    try clean_up(test_storage);
}

test "NodeIndexStorage#open when there is a storage file" {
    var test_storage: NodeIndexStorage = .{};

    _ = try std.fs.cwd().createFile(test_storage.path, .{
        .read = true,
        .truncate = false,
    });

    test_storage.open() catch {
        const out = std.io.getStdOut().writer();
        try std.fmt.format(out, "{s}", .{"failed to open a storage file\n"});
    };
    test_storage.close();

    try clean_up(test_storage);
}

test "NodeIndexStorage#allocate_next_id" {
    var test_storage: NodeIndexStorage = .{};
    try test_storage.open();
    defer test_storage.close();

    try testing.expect(test_storage.next_id == 0);

    var i: u32 = 0;
    while (i < 5) {
        const current_id = try test_storage.allocate_next_id();
        const pos = try test_storage.file.?.getEndPos();
        try testing.expect(current_id == i);
        try testing.expect(test_storage.next_id == i + 1);
        try testing.expect(@as(u64, test_storage.next_id) * 4 == pos);
        i += 1;
    }

    try clean_up(test_storage);
}

test "NodeIndexStorage update and get the record" {
    var test_storage: NodeIndexStorage = .{};
    try test_storage.open();
    defer test_storage.close();

    var i: u32 = 0;
    while (i < 5) {
        i += 1;
        const id = try test_storage.allocate_next_id();
        const node_pointer: NodePointer = i;
        try test_storage.update_record(id, node_pointer);
        const retrieved_pointer = try test_storage.get_record(id);
        try testing.expect(retrieved_pointer == node_pointer);
    }

    try clean_up(test_storage);
}

// TODO: test functions when file is null
