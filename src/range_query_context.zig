const std = @import("std");

const data_types = @import("./data_types.zig");
const utils = @import("./utils.zig");
const Arena = @import("./lock_free.zig").Arena;
const BinaryStorage = @import("./binary_storage.zig").BinaryStorage;
const KeyValuePair = @import("./data_types.zig").KeyValuePair;
const MergeIterator = @import("./merge_iterator.zig").MergeIterator;
const MemtableIteratorAdapter = @import("./memtable.zig").MemtableIteratorAdapter;
const SSTableCache = @import("./sstable_cache.zig").SSTableCache;
const SSTableIteratorAdapter = @import("./sstable.zig").SSTableIteratorAdapter;
const ValueType = @import("./data_types.zig").ValueType;

const BinaryDataRange = data_types.BinaryDataRange;
const FileHandle = data_types.FileHandle;
const PendingMemtableList = BinaryStorage.PendingMemtableList;
const StorageRecord = data_types.StorageRecord;

pub const RangeQueryContext = struct {
    allocator: std.mem.Allocator,
    arenas: [2]Arena,
    active_arena_idx: usize,
    range: BinaryDataRange,
    memtable_adapters: []MemtableIteratorAdapter,
    sstable_adapters: []SSTableIteratorAdapter,
    cache_records: []*SSTableCache.CacheRecord,
    pending_list_ref: ?*PendingMemtableList,
    merge_iterator: ?MergeIterator,

    pub fn init(allocator: std.mem.Allocator, storage: *BinaryStorage, range: BinaryDataRange, response_limit: u64) !RangeQueryContext {
        var memtable_list = try std.ArrayList(MemtableIteratorAdapter).initCapacity(allocator, 2);
        defer memtable_list.deinit(allocator);

        const max_level = storage.configurator.compactionMaxLevel();
        var sstable_list = try std.ArrayList(SSTableIteratorAdapter).initCapacity(allocator, max_level * 2);
        errdefer {
            for (sstable_list.items) |*adapter| {
                adapter.sstable_iterator.deinit();
            }
            sstable_list.deinit(allocator);
        }

        var cache_list = try std.ArrayList(*SSTableCache.CacheRecord).initCapacity(allocator, max_level * 2);
        errdefer {
            for (cache_list.items) |record| {
                _ = record.release();
            }
            cache_list.deinit(allocator);
        }

        var pending_ref: ?*PendingMemtableList = null;
        errdefer if (pending_ref) |ref| {
            _ = ref.release();
        };

        try collectMemtableIterators(allocator, storage, range, &memtable_list, &pending_ref);
        try collectSSTableIterators(allocator, storage, range, &sstable_list, &cache_list);

        var context = RangeQueryContext{
            .allocator = allocator,
            .arenas = .{
                try Arena.init(allocator, response_limit),
                try Arena.init(allocator, response_limit),
            },
            .active_arena_idx = 0,
            .memtable_adapters = try memtable_list.toOwnedSlice(allocator),
            .sstable_adapters = try sstable_list.toOwnedSlice(allocator),
            .cache_records = try cache_list.toOwnedSlice(allocator),
            .pending_list_ref = pending_ref,
            .merge_iterator = null,
            .range = range,
        };
        errdefer context.deinit();

        try context.initMergeIterator();
        return context;
    }

    pub fn deinit(self: *RangeQueryContext) void {
        self.arenas[0].deinit();
        self.arenas[1].deinit();

        if (self.merge_iterator) |*it| it.deinit();

        for (self.sstable_adapters) |*adapter| adapter.sstable_iterator.deinit();
        for (self.cache_records) |record| _ = record.release();
        if (self.pending_list_ref) |ref| _ = ref.release();

        if (self.cache_records.len > 0) self.allocator.free(self.cache_records);
        if (self.sstable_adapters.len > 0) self.allocator.free(self.sstable_adapters);
        if (self.memtable_adapters.len > 0) self.allocator.free(self.memtable_adapters);

        self.allocator.free(self.range.start);
        self.allocator.free(self.range.end);
    }

    pub fn next(self: *RangeQueryContext) !?StorageRecord {
        if (self.merge_iterator) |*it| return it.next();

        return null;
    }

    pub fn fetchResults(self: *RangeQueryContext, results: *std.ArrayList(KeyValuePair), results_allocator: std.mem.Allocator) !void {
        var is_spilling = false;
        while (true) {
            const record = try self.next() orelse break;

            if (record.isTombstone()) continue;
            if (record.isExpired()) continue;

            const key_extra_size = getExtraStringAllocationSize(record.key.data);
            const value_extra_size = getExtraStringAllocationSize(record.value.data);
            const size_needed = record.key.data.len + key_extra_size + record.value.data.len + value_extra_size;
            var active_arena = &self.arenas[self.active_arena_idx];

            // TODO: through an error if the record doesn't fit into arena
            if (size_needed + active_arena.currentOffset() > self.arenas[0].arena.len) {
                self.active_arena_idx = 1 - self.active_arena_idx;
                active_arena = &self.arenas[self.active_arena_idx];
                active_arena.reset();
                is_spilling = true;
            }

            const key_offset = try active_arena.reserve(record.key.data.len);
            const key_slice = active_arena.arena[key_offset .. key_offset + record.key.data.len];
            @memcpy(key_slice, record.key.data);

            const value_offset = try active_arena.reserve(record.value.data.len);
            const value_slice = active_arena.arena[value_offset .. value_offset + record.value.data.len];
            @memcpy(value_slice, record.value.data);

            try results.append(results_allocator, .{
                .key = try self.toJsonValue(key_slice),
                .value = try self.toJsonValue(value_slice),
            });

            if (is_spilling) {
                break;
            }
        }
    }

    fn toJsonValue(self: *RangeQueryContext, data: []const u8) !std.json.Value {
        const data_type = ValueType.fromBytes(data);
        const raw_data = data[1..];
        return switch (data_type) {
            .boolean => .{ .bool = utils.intFromBytes(u8, raw_data, 0) == 1 },
            .smallint => .{ .integer = @as(i64, @intCast(utils.intFromBytes(i8, raw_data, 0))) },
            .int => .{ .integer = @as(i64, @intCast(utils.intFromBytes(i32, raw_data, 0))) },
            .bigint => .{ .integer = utils.intFromBytes(i64, raw_data, 0) },
            .smallserial => .{ .integer = @as(i64, @intCast(utils.intFromBytes(u8, raw_data, 0))) },
            .serial => .{ .integer = @as(i64, @intCast(utils.intFromBytes(u32, raw_data, 0))) },
            .bigserial => blk: {
                const val = utils.intFromBytes(u64, raw_data, 0);
                if (val <= std.math.maxInt(i64)) {
                    break :blk .{ .integer = @as(i64, @intCast(val)) };
                } else {
                    const digits = utils.numDigits(u64, val);
                    const active_arena = &self.arenas[self.active_arena_idx];
                    const offset = try active_arena.reserve(digits);
                    const slice = active_arena.arena[offset .. offset + digits];
                    _ = try std.fmt.bufPrint(slice, "{d}", .{val});
                    break :blk .{ .number_string = slice };
                }
            },
            .float => .{ .float = @as(f64, @floatCast(@as(f32, @bitCast(utils.intFromBytes(u32, raw_data, 0))))) },
            .bigfloat => .{ .float = @as(f64, @floatCast(@as(f64, @bitCast(utils.intFromBytes(u64, raw_data, 0))))) },
            .string => .{ .string = raw_data },
        };
    }

    fn getExtraStringAllocationSize(data: []const u8) usize {
        if (data.len == 0) return 0;
        const data_type = ValueType.fromBytes(data);
        if (data_type == .bigserial) {
            const raw_data = data[1..];
            const val = utils.intFromBytes(u64, raw_data, 0);
            if (val > std.math.maxInt(i64)) {
                return utils.numDigits(u64, val);
            }
        }
        return 0;
    }

    fn collectMemtableIterators(
        allocator: std.mem.Allocator,
        storage: *BinaryStorage,
        range: BinaryDataRange,
        list: *std.ArrayList(MemtableIteratorAdapter),
        pending_ref: *?*PendingMemtableList,
    ) !void {
        const active = storage.active_memtable.load(.acquire);
        var active_adapter = MemtableIteratorAdapter.init(active);
        active_adapter.memtable_iterator.setRange(range);
        if (active_adapter.memtable_iterator.current != null) {
            try list.append(allocator, active_adapter);
        }

        if (storage.pending_memtables.load(.acquire)) |pending_list| {
            _ = pending_list.acquire();
            pending_ref.* = pending_list;

            var curr = pending_list.get().head;
            while (curr) |node| : (curr = node.next) {
                var adapter = MemtableIteratorAdapter.init(node.entry.memtable);
                adapter.memtable_iterator.setRange(range);
                if (adapter.memtable_iterator.current != null) {
                    try list.append(allocator, adapter);
                }
            }
        }
    }

    fn collectSSTableIterators(
        allocator: std.mem.Allocator,
        storage: *BinaryStorage,
        range: BinaryDataRange,
        sstable_list: *std.ArrayList(SSTableIteratorAdapter),
        cache_list: *std.ArrayList(*SSTableCache.CacheRecord),
    ) !void {
        const max_level = storage.configurator.compactionMaxLevel();
        for (0..max_level) |level| {
            const file_list = storage.table_file_manager.acquireFilesAtLevel(@intCast(level)) orelse continue;
            defer _ = file_list.release();

            var file_curr = file_list.get().head;
            while (file_curr) |file_node| : (file_curr = file_node.next) {
                const handle = FileHandle{ .level = @intCast(level), .file_id = file_node.entry };
                const cache_record = try storage.sstable_cache.get(handle, &storage.table_file_manager) orelse continue;
                errdefer _ = cache_record.release();

                if (!try cache_record.get().table.hasDataFromRange(range)) {
                    _ = cache_record.release();
                    continue;
                }

                var adapter = try SSTableIteratorAdapter.init(cache_record.get().table);
                errdefer adapter.sstable_iterator.deinit();

                try adapter.sstable_iterator.setRange(range);
                try cache_list.append(allocator, cache_record);
                try sstable_list.append(allocator, adapter);
            }
        }
    }

    fn initMergeIterator(self: *RangeQueryContext) !void {
        const total_sources = self.memtable_adapters.len + self.sstable_adapters.len;
        if (total_sources == 0) return;

        var source_iterators = try self.allocator.alloc(StorageRecord.Iterator, total_sources);
        defer self.allocator.free(source_iterators);

        var idx: usize = 0;
        for (self.memtable_adapters) |*adapter| {
            source_iterators[idx] = adapter.iterator();
            idx += 1;
        }
        for (self.sstable_adapters) |*adapter| {
            source_iterators[idx] = adapter.iterator();
            idx += 1;
        }

        self.merge_iterator = try MergeIterator.init(self.allocator, source_iterators);
    }
};
