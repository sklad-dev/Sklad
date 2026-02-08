const std = @import("std");

const data_types = @import("./data_types.zig");
const BinaryStorage = @import("./binary_storage.zig").BinaryStorage;
const KeyValuePair = @import("./data_types.zig").KeyValuePair;
const MergeIterator = @import("./merge_iterator.zig").MergeIterator;
const MemtableIteratorAdapter = @import("./memtable.zig").MemtableIteratorAdapter;
const SSTableCache = @import("./sstable_cache.zig").SSTableCache;
const SSTableIteratorAdapter = @import("./sstable.zig").SSTableIteratorAdapter;

const BinaryDataRange = data_types.BinaryDataRange;
const FileHandle = data_types.FileHandle;
const PendingMemtableList = BinaryStorage.PendingMemtableList;
const StorageRecord = data_types.StorageRecord;

pub const RangeQueryContext = struct {
    allocator: std.mem.Allocator,
    batch_size: usize = 100,
    range: BinaryDataRange,
    memtable_adapters: []MemtableIteratorAdapter,
    sstable_adapters: []SSTableIteratorAdapter,
    cache_records: []*SSTableCache.CacheRecord,
    pending_list_ref: ?*PendingMemtableList,
    merge_iterator: ?MergeIterator,

    pub fn init(allocator: std.mem.Allocator, storage: *BinaryStorage, range: BinaryDataRange) !RangeQueryContext {
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
        if (self.merge_iterator) |*it| it.deinit();

        for (self.sstable_adapters) |*adapter| {
            adapter.sstable_iterator.deinit();
        }
        for (self.cache_records) |record| {
            _ = record.release();
        }
        if (self.pending_list_ref) |ref| {
            _ = ref.release();
        }

        if (self.cache_records.len > 0) self.allocator.free(self.cache_records);
        if (self.sstable_adapters.len > 0) self.allocator.free(self.sstable_adapters);
        if (self.memtable_adapters.len > 0) self.allocator.free(self.memtable_adapters);

        self.allocator.free(self.range.start);
        self.allocator.free(self.range.end);
    }

    pub fn next(self: *RangeQueryContext) !?StorageRecord {
        if (self.merge_iterator) |*it| {
            return it.next();
        }
        return null;
    }

    pub fn fetchResults(self: *RangeQueryContext, results: *std.ArrayList(KeyValuePair)) !void {
        var count: u64 = 0;
        while (count < self.batch_size) : (count += 1) {
            const record = try self.next() orelse break;

            if (record.isTombstone()) continue;
            if (record.isExpired()) continue;

            const key_copy = try self.allocator.alloc(u8, record.key.data.len);
            errdefer self.allocator.free(key_copy);
            @memcpy(key_copy, record.key.data);

            const value_copy = try self.allocator.alloc(u8, record.value.data.len);
            errdefer self.allocator.free(value_copy);
            @memcpy(value_copy, record.value.data);

            try results.append(self.allocator, .{
                .key = key_copy,
                .value = value_copy,
            });
        }
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
