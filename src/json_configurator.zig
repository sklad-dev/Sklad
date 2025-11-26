const std = @import("std");
const Configurator = @import("./configurator.zig").Configurator;

pub const DEFAULT_CONFIGURATION_FILE_PATH = "config/configuration.json";

pub const JsonConfigurator = struct {
    config: Configuration,

    const Configuration = struct {
        memtable: MemtableConfiguration,
        sstable: SSTableConfiguration,
        sstable_cache: SSTableCacheConfiguration,
        compaction: CompactionConfiguration,
        worker_pool: WorkerPoolConfiguration,
    };

    const MemtableConfiguration = struct {
        max_size: u64,
        max_level: u8,
    };

    const SSTableConfiguration = struct {
        block_size: u32,
        bloom_bits_per_key: u8,
    };

    const SSTableCacheConfiguration = struct {
        size: u8,
    };

    const CompactionConfiguration = struct {
        tiered: TieredCompactionConfiguration,
    };

    const TieredCompactionConfiguration = struct {
        max_level: u8,
        level_multiplier: u8,
        level_threshold: u8,
    };

    const WorkerPoolConfiguration = struct {
        min_workers: u8,
        max_workers: u8,
        idle_timeout_seconds: i64,
        task_wait_threshold_us: u64,
    };

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !JsonConfigurator {
        const data = try std.fs.cwd().readFileAlloc(allocator, path, 1024);
        defer allocator.free(data);

        const config = try std.json.parseFromSlice(
            Configuration,
            allocator,
            data,
            .{ .allocate = .alloc_always },
        );
        defer config.deinit();

        return JsonConfigurator{ .config = config.value };
    }

    pub fn configurator(self: *JsonConfigurator) Configurator {
        return .{
            .ptr = self,
            .memtable_max_size_fn = memtableMaxSize,
            .memtable_max_level_fn = memtableMaxLevel,
            .sstable_block_size_fn = sstableBlockSize,
            .sstable_bloom_bits_per_key_fn = sstableBloomBitsPerKey,
            .sstable_cache_size_fn = sstableCacheSize,
            .compaction_max_level_fn = compactionMaxLevel,
            .compaction_level_multiplier_fn = compactionLevelMultiplier,
            .compaction_level_threshold_fn = compactionLevelThreshold,
            .worker_pool_min_workers_fn = minWorkers,
            .worker_pool_max_workers_fn = maxWorkers,
            .worker_pool_idle_timeout_seconds_fn = idleTimeout,
            .worker_pool_task_wait_threshold_us_fn = taskWaitThreshold,
        };
    }

    pub fn memtableMaxSize(ptr: *anyopaque) u64 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.memtable.max_size;
    }

    pub fn memtableMaxLevel(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.memtable.max_level;
    }

    pub fn sstableBlockSize(ptr: *anyopaque) u32 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.sstable.block_size;
    }

    pub fn sstableBloomBitsPerKey(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.sstable.bloom_bits_per_key;
    }

    pub fn sstableCacheSize(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.sstable_cache.size;
    }

    pub fn compactionMaxLevel(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.compaction.tiered.max_level;
    }

    pub fn compactionLevelMultiplier(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.compaction.tiered.level_multiplier;
    }

    pub fn compactionLevelThreshold(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.compaction.tiered.level_threshold;
    }

    pub fn minWorkers(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.worker_pool.min_workers;
    }

    pub fn maxWorkers(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.worker_pool.max_workers;
    }

    pub fn idleTimeout(ptr: *anyopaque) i64 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.worker_pool.idle_timeout_seconds;
    }

    pub fn taskWaitThreshold(ptr: *anyopaque) u64 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.worker_pool.task_wait_threshold_us;
    }
};
