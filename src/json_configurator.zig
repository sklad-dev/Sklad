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
        max_size: u64,
        max_level: u8,
    };

    const SSTableConfiguration = struct {
        block_size: u32,
        bloom_bits_per_key: u8,
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
            .memtable_max_size_fn = memtableMaxSize,
            .memtable_max_level_fn = memtableMaxLevel,
            .sstable_block_size_fn = sstableBlockSize,
            .sstable_bloom_bits_per_key_fn = sstableBloomBitsPerKey,
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
};
