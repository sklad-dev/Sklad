pub const Configurator = struct {
    ptr: *anyopaque,
    memtable_max_size_fn: *const fn (ptr: *anyopaque) u64,
    memtable_max_level_fn: *const fn (ptr: *anyopaque) u8,
    sstable_sparse_index_step_fn: *const fn (ptr: *anyopaque) u32,
    sstable_bloom_bits_per_key_fn: *const fn (ptr: *anyopaque) u32,

    pub fn memtableMaxSize(self: *const Configurator) u64 {
        return self.memtable_max_size_fn(self.ptr);
    }

    pub fn memtableMaxLevel(self: *const Configurator) u8 {
        return self.memtable_max_level_fn(self.ptr);
    }

    pub fn sstableSparseIndexStep(self: *const Configurator) u32 {
        return self.sstable_sparse_index_step_fn(self.ptr);
    }

    pub fn sstableBloomBitsPerKey(self: *const Configurator) u32 {
        return self.sstable_bloom_bits_per_key_fn(self.ptr);
    }
};

pub const TestingConfigurator = struct {
    max_size: u64,
    max_level: u8,
    index_step: u32,
    bits_per_key: u32,

    pub fn init() TestingConfigurator {
        return .{
            .max_size = 1536,
            .max_level = 2,
            .index_step = 44,
            .bits_per_key = 20,
        };
    }

    pub fn configurator(self: *TestingConfigurator) Configurator {
        return .{
            .ptr = self,
            .memtable_max_size_fn = memtableMaxSize,
            .memtable_max_level_fn = memtableMaxLevel,
            .sstable_sparse_index_step_fn = sstableSparseIndexStep,
            .sstable_bloom_bits_per_key_fn = sstableBloomBitsPerKey,
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

    pub fn sstableSparseIndexStep(ptr: *anyopaque) u32 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.index_step;
    }

    pub fn sstableBloomBitsPerKey(ptr: *anyopaque) u32 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.bits_per_key;
    }
};
