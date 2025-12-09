const std = @import("std");
const utils = @import("utils.zig");
const AppendOnlyQueue = @import("lock_free.zig").AppendOnlyQueue;
const FileHandle = @import("data_types.zig").FileHandle;
const RefCounted = @import("lock_free.zig").RefCounted;

pub const ManifestEntryType = enum(u8) {
    fileAdded,
    fileRemoved,
    cleanupCheckpoint,
};

pub const ManifestEntry = struct {
    entry_type: ManifestEntryType,
    level: u8,
    file_id: u64,
    timestamp: i64,
};
pub const MANIFEST_ENTRY_SIZE = @sizeOf(u8) + @sizeOf(u8) + @sizeOf(u64) + @sizeOf(i64);

pub const Manifest = struct {
    const EntryBuffer = RefCounted(AppendOnlyQueue(ManifestEntry, null));

    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,
    buffer: std.atomic.Value(*EntryBuffer),
    _padding1: u8 align(std.atomic.cache_line) = 0,
    is_flushing: std.atomic.Value(bool),
    _padding2: u8 align(std.atomic.cache_line) = 0,

    pub const RemovedFileEntriesIterator = struct {
        reader: std.fs.File.Reader,
        current_offset: u64,
        end_offset: u64,
        reader_buffer: [8]u8 = undefined,

        pub fn init(manifest: *Manifest, last_checkpoint_offset: u64) !RemovedFileEntriesIterator {
            var iterator = RemovedFileEntriesIterator{
                .reader = undefined,
                .current_offset = last_checkpoint_offset,
                .end_offset = try manifest.file.getEndPos(),
            };
            iterator.reader = manifest.file.reader(&iterator.reader_buffer);
            return iterator;
        }

        pub fn next(self: *RemovedFileEntriesIterator) !?ManifestEntry {
            if (self.end_offset == 0) return null;
            if (self.current_offset >= self.end_offset) return null;

            while (self.current_offset < self.end_offset) {
                const offset = self.current_offset;
                self.current_offset += MANIFEST_ENTRY_SIZE;

                try self.reader.seekTo(offset);
                const entry_type: ManifestEntryType = @enumFromInt(try utils.readNumber(u8, &self.reader.interface));
                if (entry_type == .fileRemoved) {
                    try self.reader.seekTo(offset + 1);
                    const level: u8 = try utils.readNumber(u8, &self.reader.interface);
                    try self.reader.seekTo(offset + 2);
                    const file_id: u64 = try utils.readNumber(u64, &self.reader.interface);
                    try self.reader.seekTo(offset + 10);
                    const timestamp: i64 = try utils.readNumber(i64, &self.reader.interface);
                    return ManifestEntry{
                        .entry_type = entry_type,
                        .level = level,
                        .file_id = file_id,
                        .timestamp = timestamp,
                    };
                }
            }

            return null;
        }
    };

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Manifest {
        const manifest_path = try std.fmt.allocPrint(allocator, "{s}/MANIFEST", .{path});
        errdefer allocator.free(manifest_path);

        const file = try std.fs.cwd().createFile(manifest_path, .{
            .read = true,
            .truncate = false,
        });

        const initial_buffer = try allocator.create(EntryBuffer);
        initial_buffer.* = EntryBuffer.init(
            allocator,
            AppendOnlyQueue(ManifestEntry, null).init(allocator),
        );

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
        _ = buffer.release();
        self.file.close();
        self.allocator.free(self.path);
    }

    pub fn addFile(self: *Manifest, level: u8, file_id: u64) void {
        const record = ManifestEntry{
            .entry_type = .fileAdded,
            .level = level,
            .file_id = file_id,
            .timestamp = std.time.microTimestamp(),
        };
        self.append(record);
    }

    pub fn removeFile(self: *Manifest, level: u8, file_id: u64) void {
        const record = ManifestEntry{
            .entry_type = .fileRemoved,
            .level = level,
            .file_id = file_id,
            .timestamp = std.time.microTimestamp(),
        };
        self.append(record);
    }

    pub fn recordCleanupCheckpoint(self: *Manifest, last_cleaned_offset: u64) void {
        const record = ManifestEntry{
            .entry_type = .cleanupCheckpoint,
            .level = 0, // Unused for checkpoint
            .file_id = last_cleaned_offset,
            .timestamp = std.time.microTimestamp(),
        };
        self.append(record);
    }

    pub fn recover(self: *Manifest, deleted_files: *std.AutoHashMap(FileHandle, void)) !void {
        var last_checkpoint_offset: u64 = 0;
        var reader_buffer: [8]u8 = undefined;
        var reader = self.file.reader(&reader_buffer);
        const end_position = try self.file.getEndPos();
        if (end_position >= MANIFEST_ENTRY_SIZE) {
            var offset: u64 = end_position - MANIFEST_ENTRY_SIZE;
            while (offset >= last_checkpoint_offset) {
                try reader.seekTo(offset);
                const entry_type: ManifestEntryType = @enumFromInt(try utils.readNumber(u8, &reader.interface));
                try reader.seekTo(offset + 1);
                const level: u8 = try utils.readNumber(u8, &reader.interface);
                try reader.seekTo(offset + 2);
                const file_id: u64 = try utils.readNumber(u64, &reader.interface);

                switch (entry_type) {
                    .fileRemoved => {
                        _ = try deleted_files.put(.{
                            .level = level,
                            .file_id = file_id,
                        }, {});
                    },
                    .cleanupCheckpoint => {
                        last_checkpoint_offset = file_id;
                    },
                    else => {},
                }

                if (offset < MANIFEST_ENTRY_SIZE) break;
                offset -= MANIFEST_ENTRY_SIZE;
            }
        }
    }

    pub fn removedFileEntriesIterator(self: *Manifest) !RemovedFileEntriesIterator {
        return try RemovedFileEntriesIterator.init(
            self,
            try self.findLastCheckpointOffset(),
        );
    }

    fn findLastCheckpointOffset(self: *Manifest) !u64 {
        const end_position = try self.file.getEndPos();
        if (end_position == 0) return 0;

        var last_checkpoint_offset: u64 = 0;
        var reader_buffer: [8]u8 = undefined;
        var reader = self.file.reader(&reader_buffer);

        if (end_position >= MANIFEST_ENTRY_SIZE) {
            var offset: u64 = end_position - MANIFEST_ENTRY_SIZE;
            while (offset >= last_checkpoint_offset) {
                try reader.seekTo(offset);
                const entry_type: ManifestEntryType = @enumFromInt(try utils.readNumber(u8, &reader.interface));

                if (entry_type == .cleanupCheckpoint) {
                    try reader.seekTo(offset + 2);
                    last_checkpoint_offset = try utils.readNumber(u64, &reader.interface);
                    break;
                }

                if (offset < MANIFEST_ENTRY_SIZE) break;
                offset -= MANIFEST_ENTRY_SIZE;
            }
        }

        return last_checkpoint_offset;
    }

    fn append(self: *Manifest, record: ManifestEntry) void {
        const current_buffer = self.buffer.load(.acquire);
        _ = current_buffer.acquire();
        defer _ = current_buffer.release();

        current_buffer.get().enqueue(record);
    }

    pub fn flush(self: *Manifest) !bool {
        if (self.is_flushing.swap(true, .acquire)) return false;
        defer self.is_flushing.store(false, .release);

        const new_buffer = try self.allocator.create(EntryBuffer);
        new_buffer.* = EntryBuffer.init(
            self.allocator,
            AppendOnlyQueue(ManifestEntry, null).init(self.allocator),
        );

        const old_buffer = self.buffer.swap(new_buffer, .acq_rel);
        errdefer {
            const current_buffer = self.buffer.load(.acquire);
            _ = current_buffer.acquire();
            defer _ = current_buffer.release();

            var curr = old_buffer.get().head.next;
            while (curr) |node| : (curr = node.next) {
                if (node.entry) |entry| {
                    current_buffer.get().enqueue(entry);
                }
            }
        }
        defer _ = old_buffer.release();

        const max_position = try self.file.getEndPos();
        var writer = self.file.writer(&[0]u8{});
        try writer.seekTo(max_position);

        var current = old_buffer.get().head.next;
        while (current) |node| : (current = node.next) {
            if (node.entry) |entry| {
                try utils.writeNumber(u8, &writer.interface, @intFromEnum(entry.entry_type));
                try utils.writeNumber(u8, &writer.interface, entry.level);
                try utils.writeNumber(u64, &writer.interface, entry.file_id);
                try utils.writeNumber(i64, &writer.interface, entry.timestamp);
            }
        }

        try self.file.sync();

        return true;
    }
};

// Tests
const testing = std.testing;

test "Manifest#append and flush" {
    const allocator = testing.allocator;
    var manifest = try Manifest.init(allocator, "./");
    defer manifest.deinit();

    manifest.addFile(0, 42);
    manifest.removeFile(1, 84);
    try testing.expect(try manifest.flush());

    var buffer: [MANIFEST_ENTRY_SIZE]u8 = undefined;
    var reader = manifest.file.reader(&buffer);
    try reader.seekTo(0);

    const entry_type1: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(1);
    const level1: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(2);
    const file_id1: u64 = try utils.readNumber(u64, &reader.interface);

    try testing.expect(entry_type1 == @intFromEnum(ManifestEntryType.fileAdded));
    try testing.expect(level1 == 0);
    try testing.expect(file_id1 == 42);

    try reader.seekTo(MANIFEST_ENTRY_SIZE);
    const entry_type2: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(19);
    const level2: u8 = try utils.readNumber(u8, &reader.interface);
    try reader.seekTo(20);
    const file_id2: u64 = try utils.readNumber(u64, &reader.interface);

    try testing.expect(entry_type2 == @intFromEnum(ManifestEntryType.fileRemoved));
    try testing.expect(level2 == 1);
    try testing.expect(file_id2 == 84);

    try std.fs.cwd().deleteFile("./MANIFEST");
}
