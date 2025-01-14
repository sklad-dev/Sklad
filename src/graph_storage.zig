const std = @import("std");
const utils = @import("./utils.zig");
const NodeStorage = @import("./node_storage.zig").NodeStorage;
const ConnectionStorage = @import("./connection_storage.zig").ConnectionStorage;

const DATABASE_STORAGE = ".hodag";

pub const GraphStorage = struct {
    allocator: std.mem.Allocator,
    node_storage: NodeStorage,
    connection_storage: ConnectionStorage,

    pub fn init(allocator: std.mem.Allocator, max_node_memtable_size: u16, max_connection_memtable_size: u16) !GraphStorage {
        try utils.make_dir_if_not_exists(DATABASE_STORAGE);

        return GraphStorage{
            .allocator = allocator,
            .node_storage = try NodeStorage.init(allocator, DATABASE_STORAGE ++ "/nodes", max_node_memtable_size),
            .connection_storage = try ConnectionStorage.init(allocator, DATABASE_STORAGE ++ "/connections", max_connection_memtable_size),
        };
    }

    pub inline fn stop(self: *GraphStorage) void {
        self.node_storage.stop();
        self.connection_storage.stop();
    }
};
