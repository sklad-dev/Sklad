pub const Configurator = struct {
    ptr: *anyopaque,
    memtable_max_size_fn: *const fn (ptr: *anyopaque) u64,
    memtable_max_level_fn: *const fn (ptr: *anyopaque) u8,
    sstable_block_size_fn: *const fn (ptr: *anyopaque) u32,
    sstable_bloom_bits_per_key_fn: *const fn (ptr: *anyopaque) u8,
    compaction_max_level_fn: *const fn (ptr: *anyopaque) u8,
    compaction_level_multiplier_fn: *const fn (ptr: *anyopaque) u8,
    compaction_level_threshold_fn: *const fn (ptr: *anyopaque) u8,

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

    pub fn compactionMaxLevel(self: *const Configurator) u8 {
        return self.compaction_max_level_fn(self.ptr);
    }

    pub fn compactionLevelMultiplier(self: *const Configurator) u8 {
        return self.compaction_level_multiplier_fn(self.ptr);
    }

    pub fn compactionLevelThreshold(self: *const Configurator) u8 {
        return self.compaction_level_threshold_fn(self.ptr);
    }
};

pub const TestingConfigurator = struct {
    max_size: u64,
    max_level: u8,
    index_step: u32,
    bits_per_key: u8,
    compaction_max_level: u8,
    compaction_level_multiplier: u8,
    compaction_level_threshold: u8,

    pub fn init() TestingConfigurator {
        return .{
            .max_size = 1536,
            .max_level = 2,
            .index_step = 44,
            .bits_per_key = 20,
            .compaction_max_level = 3,
            .compaction_level_multiplier = 4,
            .compaction_level_threshold = 4,
        };
    }

    pub fn configurator(self: *TestingConfigurator) Configurator {
        return .{
            .ptr = self,
            .memtable_max_size_fn = memtableMaxSize,
            .memtable_max_level_fn = memtableMaxLevel,
            .sstable_block_size_fn = sstableBlockSize,
            .sstable_bloom_bits_per_key_fn = sstableBloomBitsPerKey,
            .compaction_max_level_fn = compactionMaxLevel,
            .compaction_level_multiplier_fn = compactionLevelMultiplier,
            .compaction_level_threshold_fn = compactionLevelThreshold,
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
        return self.index_step;
    }

    pub fn sstableBloomBitsPerKey(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.bits_per_key;
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
};
