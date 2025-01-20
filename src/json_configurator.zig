const std = @import("std");
const Configurator = @import("./configurator.zig").Configurator;

pub const DEFAULT_CONFIGURATION_FILE_PATH = "config/configuration.json";

pub const JsonConfigurator = struct {
    config: Configuration,

    const Configuration = struct {
        memtable: MemtableConfiguration,
        sstable: SSTableConfiguration,
    };

    const MemtableConfiguration = struct {
        max_size: u16,
        max_level: u8,
        level_probability: f32,
    };

    const SSTableConfiguration = struct {
        sparse_index_step: u32,
        bloom_bits_per_key: u32,
    };

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !JsonConfigurator {
        const data = try std.fs.cwd().readFileAlloc(allocator, path, 512);
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
            .memtable_max_size_fn = memtable_max_size,
            .memtable_max_level_fn = memtable_max_level,
            .memtable_level_probability_fn = memtable_level_probability,
            .sstable_sparse_index_step_fn = sstable_sparse_index_step,
            .sstable_bloom_bits_per_key_fn = sstable_bloom_bits_per_key,
        };
    }

    pub fn memtable_max_size(ptr: *anyopaque) u16 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.memtable.max_size;
    }

    pub fn memtable_max_level(ptr: *anyopaque) u8 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.memtable.max_level;
    }

    pub fn memtable_level_probability(ptr: *anyopaque) f32 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.memtable.level_probability;
    }

    pub fn sstable_sparse_index_step(ptr: *anyopaque) u32 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.sstable.sparse_index_step;
    }

    pub fn sstable_bloom_bits_per_key(ptr: *anyopaque) u32 {
        const self: *JsonConfigurator = @ptrCast(@alignCast(ptr));
        return self.config.sstable.bloom_bits_per_key;
    }
};
