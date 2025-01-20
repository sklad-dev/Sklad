pub const Configurator = struct {
    ptr: *anyopaque,
    memtable_max_size_fn: *const fn (ptr: *anyopaque) u16,
    memtable_max_level_fn: *const fn (ptr: *anyopaque) u8,
    memtable_level_probability_fn: *const fn (ptr: *anyopaque) f32,
    sstable_sparse_index_step_fn: *const fn (ptr: *anyopaque) u32,
    sstable_bloom_bits_per_key_fn: *const fn (ptr: *anyopaque) u32,

    pub fn memtable_max_size(self: *const Configurator) u16 {
        return self.memtable_max_size_fn(self.ptr);
    }

    pub fn memtable_max_level(self: *const Configurator) u8 {
        return self.memtable_max_level_fn(self.ptr);
    }

    pub fn memtable_level_probability(self: *const Configurator) f32 {
        return self.memtable_level_probability_fn(self.ptr);
    }

    pub fn sstable_sparse_index_step(self: *const Configurator) u32 {
        return self.sstable_sparse_index_step_fn(self.ptr);
    }

    pub fn sstable_bloom_bits_per_key(self: *const Configurator) u32 {
        return self.sstable_bloom_bits_per_key_fn(self.ptr);
    }
};

pub const TestingConfigurator = struct {
    max_size: u16,
    max_level: u8,
    level_probability: f32,
    index_step: u32,
    bits_per_key: u32,

    pub fn init() TestingConfigurator {
        return .{
            .max_size = 8,
            .max_level = 2,
            .level_probability = 0.25,
            .index_step = 44,
            .bits_per_key = 20,
        };
    }

    pub fn configurator(self: *TestingConfigurator) Configurator {
        return .{
            .ptr = self,
            .memtable_max_size_fn = memtable_max_size,
            .memtable_max_level_fn = memtable_max_level,
            .memtable_level_probability_fn = memtable_level_probability,
            .sstable_sparse_index_step_fn = sstable_sparse_index_step,
            .sstable_bloom_bits_per_key_fn = sstable_bloom_bits_per_key,
        };
    }

    pub fn memtable_max_size(ptr: *anyopaque) u16 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.max_size;
    }

    pub fn memtable_max_level(ptr: *anyopaque) u8 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.max_level;
    }

    pub fn memtable_level_probability(ptr: *anyopaque) f32 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.level_probability;
    }

    pub fn sstable_sparse_index_step(ptr: *anyopaque) u32 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.index_step;
    }

    pub fn sstable_bloom_bits_per_key(ptr: *anyopaque) u32 {
        const self: *TestingConfigurator = @ptrCast(@alignCast(ptr));
        return self.bits_per_key;
    }
};
