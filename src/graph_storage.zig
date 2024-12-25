const std = @import("std");
const NodeStorage = @import("./node_storage.zig").NodeStorage;
const ConnectionStorage = @import("./connection_storage.zig").ConnectionStorage;

pub const GraphStorage = struct {
    allocator: std.mem.Allocator,
    node_storage: NodeStorage,
    connection_storage: ConnectionStorage,

    pub fn init(allocator: std.mem.Allocator, max_node_memtable_size: u16, max_connection_memtable_size: u16) !GraphStorage {
        return GraphStorage{
            .allocator = allocator,
            .node_storage = try NodeStorage.init(allocator, "./nodes", max_node_memtable_size),
            .connection_storage = try ConnectionStorage.init(allocator, "./connections", max_connection_memtable_size),
        };
    }
};
