const std = @import("std");

const global_context = @import("./global_context.zig");
const GraphStorage = @import("./graph_storage.zig").GraphStorage;
const io = @import("./io.zig");
const parse = @import("./parse.zig");
const utils = @import("./utils.zig");
const Task = @import("./task_queue.zig").Task;

pub const ExecutionError = error{
    ExecutionFailed,
    NodeNotFound,
};

pub const ExecuteTask = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    query: []u8,
    expression: parse.Expression,
    executor: Executor,

    pub fn init(allocator: std.mem.Allocator, io_context: io.IO.IoContext, query: []u8, expression: parse.Expression) !ExecuteTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .query = query,
            .expression = expression,
            .executor = Executor.init(allocator, io_context),
        };
    }

    pub fn task(self: *ExecuteTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        defer std.posix.close(self.io_context.socket);

        self.executor.execute(&self.expression) catch |e| {
            std.log.err("Error! Query execution failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.send_response(i8, ExecutionError, self.allocator, -1, ExecutionError.ExecutionFailed);
        };

        self.io_context.send_response(i8, ExecutionError, self.allocator, 0, null);
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ExecuteTask = @ptrCast(@alignCast(ptr));
        switch (self.expression) {
            .insert => self.expression.insert.destory(),
            .insert_connection => self.expression.insert_connection.destory(),
            .find => self.expression.find.destory(),
        }
        self.executor.deinit();
        allocator.free(self.query);
        allocator.destroy(self);
    }
};

const Executor = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    inserted_nodes: std.HashMap(*const parse.NodeDefinitionNode, u64, parse.NodeDefinitionNode.HashContext, std.hash_map.default_max_load_percentage),
    graph_storage: *GraphStorage,

    pub fn init(allocator: std.mem.Allocator, io_context: io.IO.IoContext) Executor {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .inserted_nodes = std.HashMap(
                *const parse.NodeDefinitionNode,
                u64,
                parse.NodeDefinitionNode.HashContext,
                std.hash_map.default_max_load_percentage,
            ).init(allocator),
            .graph_storage = global_context.get_graph_storage().?,
        };
    }

    pub fn deinit(self: *Executor) void {
        self.inserted_nodes.deinit();
    }

    pub fn execute(self: *Executor, expression: *parse.Expression) !void {
        switch (expression.*) {
            .insert => try self.execute_insert_expression(&expression.*.insert),
            .insert_connection => try self.execute_insert_connection_expression(&expression.*.insert_connection),
            .find => try self.execute_find_expression(&expression.*.find),
        }
    }

    fn execute_insert_expression(self: *Executor, expression: *parse.InsertExpression) !void {
        for (expression.nodes.items) |node| {
            const node_id = try self.graph_storage.node_storage.put(node.value, node.value_type);
            try self.inserted_nodes.put(&node, node_id);
        }

        for (expression.connections.items) |connection| {
            try self.insert_connection(parse.ExpressionType.insert, &connection);
        }
    }

    fn execute_insert_connection_expression(self: *Executor, expression: *parse.InsertConnectionExpression) !void {
        for (expression.connections.items) |connection| {
            try self.insert_connection(parse.ExpressionType.insert_connection, &connection);
        }
    }

    fn execute_find_expression(self: *Executor, expression: *parse.FindExpression) !void {
        var iter = expression.identifiers.iterator();
        while (iter.next()) |identifier| {
            const condition = identifier.value_ptr.*.value_conditions.items[0];
            switch (condition) {
                .value_condition => {
                    switch (condition.value_condition.operator) {
                        .equal => {
                            const node_id = try self.graph_storage.node_storage.find(
                                condition.value_condition.value,
                                condition.value_condition.value_type,
                            );
                            self.io_context.send_response(u64, io.IO.IoError, self.allocator, node_id orelse 0xFFFFFFFFFFFFFFFF, null);
                        },
                        else => return utils.SupportingError.NotImplemented,
                    }
                },
                .in_condition => return utils.SupportingError.NotImplemented,
            }
        }
    }

    fn get_node_id(self: *Executor, comptime E: parse.ExpressionType, node: *const parse.NodeDefinitionNode) !?u64 {
        switch (E) {
            inline .insert => {
                if (self.inserted_nodes.get(node)) |node_id| {
                    return node_id;
                }
            },
            inline .insert_connection => {},
            inline .find => {},
        }

        return try global_context
            .get_graph_storage().?
            .node_storage
            .find(node.value, node.value_type);
    }

    fn insert_connection(self: *Executor, comptime E: parse.ExpressionType, connection: *const parse.ConnectionNode) !void {
        const src_node_id = try self.get_node_id(E, &connection.source);
        if (src_node_id == null) return ExecutionError.NodeNotFound;

        const dst_node_id = try self.get_node_id(E, &connection.destination);
        if (dst_node_id == null) return ExecutionError.NodeNotFound;

        var label_node_id: u64 = 0xFFFFFFFFFFFFFFFF;
        if (connection.label) |label_node| {
            label_node_id = try self.get_node_id(E, &label_node) orelse 0xFFFFFFFFFFFFFFFF;
        }

        try self.graph_storage.connection_storage.put(src_node_id.?, dst_node_id.?, label_node_id);
    }
};
