const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const lex = @import("./lex.zig");
const utils = @import("./utils.zig");

const ValueType = @import("./data_types.zig").ValueType;
const Task = @import("./task_queue.zig").Task;
const ExecuteTask = @import("./execute.zig").ExecuteTask;

pub const ExpressionType = enum {
    insert,
    insert_connection,
};

pub const Expression = union(ExpressionType) {
    insert: InsertExpression,
    insert_connection: InsertConnectionExpression,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !Expression {
        query.current_pos = 0;
        if (query.next_token()) |token| {
            switch (token.kind) {
                .insert_keyword => {
                    if (query.peak_next_token()) |t| {
                        switch (t.kind) {
                            .connection_keyword => {
                                query.current_pos += 1;
                                return Expression{
                                    .insert_connection = try InsertConnectionExpression.parse(allocator, query),
                                };
                            },
                            .left_square_bracket => return Expression{
                                .insert = try InsertExpression.parse(allocator, query),
                            },
                            else => return ParserError.UnexpectedToken,
                        }
                    } else {
                        return ParserError.UnexpectedToken;
                    }
                },
                else => return ParserError.UnexpectedToken,
            }
        } else {
            return ParserError.UnexpectedToken;
        }
    }
};

pub const InsertExpression = struct {
    allocator: std.mem.Allocator,
    nodes: std.ArrayList(NodeDefinitionNode),
    connections: std.ArrayList(ConnectionNode),

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !InsertExpression {
        var nodes = std.ArrayList(NodeDefinitionNode).init(allocator);
        var connections = std.ArrayList(ConnectionNode).init(allocator);
        while (query.peak_next_token()) |token| {
            switch (token.kind) {
                .left_square_bracket => try nodes.append(try NodeDefinitionNode.parse(allocator, query)),
                .pre_label_connection_op => {
                    _ = query.next_token();
                    try nodes.append(try NodeDefinitionNode.parse(allocator, query));
                    _ = try query.expect_token(&[_]lex.Token.Kind{.left_right_connection_op});
                    try nodes.append(try NodeDefinitionNode.parse(allocator, query));
                    try connections.append(ConnectionNode{
                        .source = nodes.items[nodes.items.len - 3],
                        .destination = nodes.items[nodes.items.len - 1],
                        .label = nodes.items[nodes.items.len - 2],
                    });
                },
                .left_right_connection_op => {
                    _ = query.next_token();
                    try nodes.append(try NodeDefinitionNode.parse(allocator, query));
                    try connections.append(ConnectionNode{
                        .source = nodes.items[nodes.items.len - 2],
                        .destination = nodes.items[nodes.items.len - 1],
                        .label = null,
                    });
                },
                .comma => {
                    _ = query.next_token();
                    continue;
                },
                .semicolon => {
                    _ = query.next_token();
                    break;
                },
                else => return ParserError.UnexpectedToken,
            }
        }
        return .{
            .allocator = allocator,
            .nodes = nodes,
            .connections = connections,
        };
    }

    pub fn destory(self: *InsertExpression) void {
        self.nodes.deinit();
        self.connections.deinit();
    }
};

pub const InsertConnectionExpression = struct {
    allocator: std.mem.Allocator,
    connections: std.ArrayList(ConnectionNode),

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !InsertConnectionExpression {
        var connections = std.ArrayList(ConnectionNode).init(allocator);
        while (query.peak_next_token()) |token| {
            switch (token.kind) {
                .left_square_bracket => try connections.append(try ConnectionNode.parse(allocator, query)),
                .comma => {
                    _ = query.next_token();
                    continue;
                },
                .semicolon => {
                    _ = query.next_token();
                    break;
                },
                else => return ParserError.UnexpectedToken,
            }
        }
        return .{
            .allocator = allocator,
            .connections = connections,
        };
    }

    pub fn destory(self: *InsertConnectionExpression) void {
        self.connections.deinit();
    }
};

pub const ConnectionNode = struct {
    source: NodeDefinitionNode,
    destination: NodeDefinitionNode,
    label: ?NodeDefinitionNode,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !ConnectionNode {
        const source = try NodeDefinitionNode.parse(allocator, query);
        const token = try query.expect_token(&[_]lex.Token.Kind{ .pre_label_connection_op, .left_right_connection_op });
        var label: ?NodeDefinitionNode = null;
        if (token.kind == .pre_label_connection_op) {
            label = try NodeDefinitionNode.parse(allocator, query);
            _ = try query.expect_token(&[_]lex.Token.Kind{.left_right_connection_op});
        }
        const destination = try NodeDefinitionNode.parse(allocator, query);

        return .{
            .source = source,
            .label = label,
            .destination = destination,
        };
    }
};

pub const NodeDefinitionNode = struct {
    allocator: std.mem.Allocator,
    value_type: ValueType,
    value: []u8,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !NodeDefinitionNode {
        _ = try query.expect_token(&[_]lex.Token.Kind{.left_square_bracket});
        const value_type = try value_type_from_type_specifier_token(
            try query.expect_token(&[_]lex.Token.Kind{.type_specifier}),
        );
        const value_token = try query.expect_token(
            &[_]lex.Token.Kind{
                .string_value,
                .numeric_value,
                .true_keyword,
                .false_keyword,
            },
        );
        _ = try query.expect_token(&[_]lex.Token.Kind{.right_square_bracket});
        return .{
            .allocator = allocator,
            .value_type = value_type,
            .value = try value_from_str(allocator, value_type, value_token.string()),
        };
    }

    pub fn deinit(self: NodeDefinitionNode) void {
        self.allocator.free(self.value);
    }

    fn value_type_from_type_specifier_token(token: lex.Token) !ValueType {
        if (std.meta.stringToEnum(ValueType, token.string())) |value_type| {
            return value_type;
        } else {
            return ParserError.UnknownTypeSpecifier;
        }
    }

    fn value_from_str(allocator: std.mem.Allocator, value_type: ValueType, value_str: []const u8) ![]u8 {
        const tmp: []const u8 = switch (value_type) {
            .boolean => &try utils.to_byte_key(bool, (std.mem.eql(u8, value_str, "true"))),
            .smallint => &try utils.to_byte_key(i8, try std.fmt.parseInt(i8, value_str, 10)),
            .int => &try utils.to_byte_key(i32, try std.fmt.parseInt(i32, value_str, 10)),
            .bigint => &try utils.to_byte_key(i64, try std.fmt.parseInt(i64, value_str, 10)),
            .smallserial => &try utils.to_byte_key(u8, try std.fmt.parseInt(u8, value_str, 10)),
            .serial => &try utils.to_byte_key(u32, try std.fmt.parseInt(u32, value_str, 10)),
            .bigserial => &try utils.to_byte_key(u64, try std.fmt.parseInt(u64, value_str, 10)),
            .float => &try utils.to_byte_key(f32, try std.fmt.parseFloat(f32, value_str)),
            .bigfloat => &try utils.to_byte_key(f64, try std.fmt.parseFloat(f64, value_str)),
            .string => value_str,
        };
        const value = try allocator.alloc(u8, tmp.len);
        @memcpy(value, tmp);
        return value;
    }

    pub const HashContext = struct {
        pub fn hash(self: @This(), node: *const NodeDefinitionNode) u64 {
            _ = self;
            var h = std.hash.Wyhash.init(0);
            h.update(node.value);
            h.update(std.mem.asBytes(&@intFromEnum(node.value_type)));
            return h.final();
        }

        pub fn eql(self: @This(), n1: *const NodeDefinitionNode, n2: *const NodeDefinitionNode) bool {
            _ = self;
            if (std.mem.eql(u8, n1.value, n2.value) and n1.value_type == n2.value_type) {
                return true;
            }
            return false;
        }
    };
};

pub const ParserError = error{
    InvalidQuery,
    UnexpectedToken,
    UnexpectedEndOfQuery,
    UnknownTypeSpecifier,
};

pub const TokenizedQuery = struct {
    allocator: std.mem.Allocator,
    tokens: *std.ArrayList(lex.Token),
    current_pos: u64,

    pub fn init(allocator: std.mem.Allocator, tokens: *std.ArrayList(lex.Token)) TokenizedQuery {
        return .{
            .allocator = allocator,
            .tokens = tokens,
            .current_pos = 0,
        };
    }

    pub fn next_token(self: *TokenizedQuery) ?lex.Token {
        if (self.current_pos >= self.tokens.items.len - 1) {
            return null;
        }
        defer self.current_pos += 1;
        return self.tokens.items[self.current_pos];
    }

    pub fn peak_next_token(self: *TokenizedQuery) ?lex.Token {
        if (self.current_pos >= self.tokens.items.len - 1) {
            return null;
        }
        return self.tokens.items[self.current_pos];
    }

    pub fn expect_token(self: *TokenizedQuery, token_kinds: []const lex.Token.Kind) !lex.Token {
        if (self.current_pos >= self.tokens.items.len - 1) {
            return ParserError.UnexpectedEndOfQuery;
        }
        defer self.current_pos += 1;

        const token = self.tokens.items[self.current_pos];
        for (token_kinds) |kind| {
            if (kind == token.kind) {
                return token;
            }
        }

        return ParserError.UnexpectedToken;
    }
};

pub const ParserTask = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    query: []u8,
    tokenized_query: TokenizedQuery,

    pub fn init(allocator: std.mem.Allocator, io_context: io.IO.IoContext, query: []u8, tokens: *std.ArrayList(lex.Token)) !ParserTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .query = query,
            .tokenized_query = TokenizedQuery.init(allocator, tokens),
        };
    }

    pub fn task(self: *ParserTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *ParserTask = @ptrCast(@alignCast(ptr));
        errdefer std.posix.close(self.io_context.socket);

        var expression = Expression.parse(self.allocator, &self.tokenized_query) catch |e| {
            std.log.err("Error! Query parsing failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.send_response(i8, ParserError, self.allocator, -1, ParserError.InvalidQuery);
            return;
        };
        errdefer {
            switch (expression) {
                .insert => expression.insert.destory(),
                .insert_connection => expression.insert_connection.destory(),
            }
        }

        const task_queue = global_context.get_task_queue();
        var execute_task = task_queue.?.allocator.create(ExecuteTask) catch |e| {
            std.log.err("Error! Failed to allocate a parser task: {any}", .{e});
            return;
        };
        execute_task.* = ExecuteTask.init(
            task_queue.?.allocator,
            self.io_context,
            self.query,
            expression,
        ) catch |e| {
            std.log.err("Error! Failed to create a parser task: {any}", .{e});
            return;
        };

        global_context.get_task_queue().?.enqueue(execute_task.task());
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ParserTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self.tokenized_query.tokens);
        allocator.destroy(self);
    }
};

// Test
const testing = std.testing;

test "Parse insert query" {
    var tokens = std.ArrayList(lex.Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_query = "insert [string 'name']->[string 'toothbrush'], [int 4], [float 3.0]-[string 'price']->[string 'bread'];";
    var lexer = lex.Lexer.init(insert_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.insert.destory();
    try testing.expect(expression.insert.nodes.items.len == 6);
    try testing.expect(expression.insert.connections.items.len == 2);
}

test "Parse insert connection query" {
    var tokens = std.ArrayList(lex.Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "insert connection [string 'name']->[string 'toothbrush'], [float 3.0]-[string 'price']->[string 'bread'];";
    var lexer = lex.Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.insert_connection.destory();
    try testing.expect(expression.insert_connection.connections.items.len == 2);
}
