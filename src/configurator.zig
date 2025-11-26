pub const Configurator = struct {
    ptr: *anyopaque,
    memtable_max_size_fn: *const fn (ptr: *anyopaque) u64,
    memtable_max_level_fn: *const fn (ptr: *anyopaque) u8,
    sstable_block_size_fn: *const fn (ptr: *anyopaque) u32,
    sstable_bloom_bits_per_key_fn: *const fn (ptr: *anyopaque) u8,
    sstable_cache_size_fn: *const fn (ptr: *anyopaque) u8,
    compaction_max_level_fn: *const fn (ptr: *anyopaque) u8,
    compaction_level_multiplier_fn: *const fn (ptr: *anyopaque) u8,
    compaction_level_threshold_fn: *const fn (ptr: *anyopaque) u8,
    worker_pool_min_workers_fn: *const fn (ptr: *anyopaque) u8,
    worker_pool_max_workers_fn: *const fn (ptr: *anyopaque) u8,
    worker_pool_idle_timeout_seconds_fn: *const fn (ptr: *anyopaque) i64,
    worker_pool_task_wait_threshold_us_fn: *const fn (ptr: *anyopaque) u64,

    pub fn memtableMaxSize(self: *const Configurator) u64 {
        return self.memtable_max_size_fn(self.ptr);
    }

    pub fn memtableMaxLevel(self: *const Configurator) u8 {
        return self.memtable_max_level_fn(self.ptr);
    }

    pub fn sstableBlockSize(self: *const Configurator) u32 {
        return self.sstable_block_size_fn(self.ptr);
    }

    pub fn sstableBloomBitsPerKey(self: *const Configurator) u8 {
        return self.sstable_bloom_bits_per_key_fn(self.ptr);
    }

    pub fn sstableCacheSize(self: *const Configurator) u8 {
        return self.sstable_cache_size_fn(self.ptr);
    }

    pub fn compactionMaxLevel(self: *const Configurator) u8 {
        return self.compaction_max_level_fn(self.ptr);
    }

    pub fn compactionLevelMultiplier(self: *const Configurator) u8 {
        return self.compaction_level_multiplier_fn(self.ptr);
    }

    pub fn compactionLevelThreshold(self: *const Configurator) u8 {
        return self.compaction_level_threshold_fn(self.ptr);
    }

    pub fn minWorkers(self: *const Configurator) u8 {
        return self.worker_pool_min_workers_fn(self.ptr);
    }

    pub fn maxWorkers(self: *const Configurator) u8 {
        return self.worker_pool_max_workers_fn(self.ptr);
    }

    pub fn idleTimeout(self: *const Configurator) i64 {
        return self.worker_pool_idle_timeout_seconds_fn(self.ptr);
    }

    pub fn taskWaitThreshold(self: *const Configurator) u64 {
        return self.worker_pool_task_wait_threshold_us_fn(self.ptr);
    }
};

pub const TestingConfigurator = struct {
    max_size: u64,
    max_level: u8,
    block_size: u32,
    bits_per_key: u8,
    sstable_cache_size: u8,
    compaction_max_level: u8,
    compaction_level_multiplier: u8,
    compaction_level_threshold: u8,
    min_workers: u8,
    max_workers: u8,
    idle_timeout_seconds: i64,
    task_wait_threshold_us: u64,

    pub fn init() TestingConfigurator {
        return .{
            .max_size = 1536,
            .max_level = 2,
            .block_size = 64,
            .bits_per_key = 20,
            .sstable_cache_size = 8,
            .compaction_max_level = 4,
            .compaction_level_multiplier = 4,
            .compaction_level_threshold = 4,
            .min_workers = 1,
            .max_workers = 1,
            .idle_timeout_seconds = 5,
            .task_wait_threshold_us = 5000,
        };
    }

    pub fn configurator(self: *TestingConfigurator) Configurator {
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
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.max_size;
    }

    pub fn memtableMaxLevel(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.max_level;
    }

    pub fn sstableBlockSize(ptr: *anyopaque) u32 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.block_size;
    }

    pub fn sstableBloomBitsPerKey(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.bits_per_key;
    }

    pub fn sstableCacheSize(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.sstable_cache_size;
    }

    pub fn compactionMaxLevel(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.compaction_max_level;
    }

    pub fn compactionLevelMultiplier(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.compaction_level_multiplier;
    }

    pub fn compactionLevelThreshold(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.compaction_level_threshold;
    }

    pub fn minWorkers(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.min_workers;
    }

    pub fn maxWorkers(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.max_workers;
    }

    pub fn idleTimeout(ptr: *anyopaque) i64 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.idle_timeout_seconds;
    }

    pub fn taskWaitThreshold(ptr: *anyopaque) u64 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.task_wait_threshold_us;
    }
};
