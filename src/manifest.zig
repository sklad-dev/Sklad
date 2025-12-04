const std = @import("std");
const utils = @import("utils.zig");
const AppendOnlyQueue = @import("lock_free.zig").AppendOnlyQueue;

pub const ManifsetEntryType = enum(u8) {
    MARK_DELETED,
};

pub const ManifestEntry = struct {
    entry_type: ManifsetEntryType,
    level: u8,
    file_number: u64,
};

pub const Manifest = struct {
    const EntryBuffer = AppendOnlyQueue(ManifestEntry, null);

    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    buffer: std.atomic.Value(*EntryBuffer),
    _padding1: u8 align(std.atomic.cache_line) = 0,
    is_flushing: std.atomic.Value(bool),
    _padding2: u8 align(std.atomic.cache_line) = 0,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Manifest {
        const manifest_path = try std.fmt.allocPrint(allocator, "{s}/MANIFEST", .{path});
        errdefer allocator.free(manifest_path);

        const file = try std.fs.cwd().createFile(manifest_path, .{
            .read = true,
            .truncate = false,
        });

        const initial_buffer = try allocator.create(EntryBuffer);
        initial_buffer.* = EntryBuffer.init(allocator);

        return .{
            .allocator = allocator,
            .path = manifest_path,
            .file = file,
            .buffer = std.atomic.Value(*EntryBuffer).init(initial_buffer),
            .is_flushing = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Manifest) void {
        const buffer = self.buffer.load(.acquire);
        buffer.deinit();
        self.allocator.destroy(buffer);
        self.file.close();
        self.allocator.free(self.path);
    }

    pub fn append(self: *Manifest, level: u8, file_number: u64) !void {
        self.buffer.load(.acquire).enqueue(.{
            .entry_type = .MARK_DELETED,
            .level = level,
            .file_number = file_number,
        });
    }

    pub fn flush(self: *Manifest) !void {
        if (self.is_flushing.swap(true, .acquire)) return;
        defer self.is_flushing.store(false, .release);

        const new_buffer = try self.allocator.create(EntryBuffer);
        new_buffer.* = EntryBuffer.init(self.allocator);

        const old_buffer = self.buffer.swap(new_buffer, .acq_rel);
        defer {
            old_buffer.deinit();
            self.allocator.destroy(old_buffer);
        }

        const max_position = try self.file.getEndPos();
        var writer = self.file.writer(&[0]u8{});
        try writer.seekTo(max_position);

        var current = old_buffer.head.next;
        while (current) |node| : (current = node.next) {
            if (node.entry) |entry| {
                try utils.writeNumber(u8, &writer.interface, @intFromEnum(entry.entry_type));
                try utils.writeNumber(u8, &writer.interface, entry.level);
                try utils.writeNumber(u64, &writer.interface, entry.file_number);
            }
        }

        try self.file.sync();
    }
};

// Tests
const testing = std.testing;

test "Manifest#append and flush" {
    const allocator = testing.allocator;
    var manifest = try Manifest.init(allocator, "./");
    defer manifest.deinit();

    try manifest.append(0, 42);
    try manifest.append(1, 84);
    try manifest.flush();

    var buffer: [10]u8 = undefined;
    var reader = manifest.file.reader(&buffer);
    try reader.seekTo(0);

    const entry_type1: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(1);
    const level1: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(2);
    const file_number1: u64 = try utils.readNumber(u64, &reader.interface);
    try reader.seekTo(10);

    try testing.expect(entry_type1 == @intFromEnum(ManifsetEntryType.MARK_DELETED));
    try testing.expect(level1 == 0);
    try testing.expect(file_number1 == 42);

    const entry_type2: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(11);
    const level2: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(12);
    const file_number2: u64 = try utils.readNumber(u64, &reader.interface);

    try testing.expect(entry_type2 == @intFromEnum(ManifsetEntryType.MARK_DELETED));
    try testing.expect(level2 == 1);
    try testing.expect(file_number2 == 84);

    try std.fs.cwd().deleteFile("./MANIFEST");
}
