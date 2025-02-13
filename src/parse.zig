const std = @import("std");

const lex = @import("./lex.zig");
const ValueType = @import("./data_types.zig").ValueType;

pub const ExpressionType = enum {
    insert,
    insert_connection,
};

pub const ExpressionNode = union(ExpressionType) {
    insert: InsertExpression,
    insert_connection: InsertConnectionExpression,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !ExpressionNode {
        query.current_pos = 0;
        if (query.next_token()) |token| {
            switch (token.kind) {
                .insert_keyword => {
                    if (query.peak_next_token()) |t| {
                        switch (t.kind) {
                            .connection_keyword => {
                                query.current_pos += 1;
                                return ExpressionNode{
                                    .insert_connection = try InsertConnectionExpression.parse(allocator, query),
                                };
                            },
                            .left_square_bracket => return ExpressionNode{
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
                .left_square_bracket => try nodes.append(try NodeDefinitionNode.parse(query)),
                .pre_label_connection_op => {
                    _ = query.next_token();
                    try nodes.append(try NodeDefinitionNode.parse(query));
                    _ = try query.expect_token(&[_]lex.Token.Kind{.left_right_connection_op});
                    try nodes.append(try NodeDefinitionNode.parse(query));
                    try connections.append(ConnectionNode{
                        .source = nodes.items[nodes.items.len - 3],
                        .destination = nodes.items[nodes.items.len - 1],
                        .label = nodes.items[nodes.items.len - 2],
                    });
                },
                .left_right_connection_op => {
                    _ = query.next_token();
                    try nodes.append(try NodeDefinitionNode.parse(query));
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
                .left_square_bracket => try connections.append(try ConnectionNode.parse(query)),
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

    pub fn parse(query: *TokenizedQuery) !ConnectionNode {
        const source = try NodeDefinitionNode.parse(query);
        const token = try query.expect_token(&[_]lex.Token.Kind{ .pre_label_connection_op, .left_right_connection_op });
        var label: ?NodeDefinitionNode = null;
        if (token.kind == .pre_label_connection_op) {
            label = try NodeDefinitionNode.parse(query);
            _ = try query.expect_token(&[_]lex.Token.Kind{.left_right_connection_op});
        }
        const destination = try NodeDefinitionNode.parse(query);

        return .{
            .source = source,
            .label = label,
            .destination = destination,
        };
    }
};

pub const NodeDefinitionNode = struct {
    value_type: ValueType,
    value: []const u8,

    pub fn parse(query: *TokenizedQuery) ParserError!NodeDefinitionNode {
        _ = try query.expect_token(&[_]lex.Token.Kind{.left_square_bracket});
        const value_type = try value_type_from_type_specifier_token(
            try query.expect_token(&[_]lex.Token.Kind{.type_specifier}),
        );
        const value_token = try query.expect_token(&[_]lex.Token.Kind{ .string_value, .numeric_value });
        _ = try query.expect_token(&[_]lex.Token.Kind{.right_square_bracket});
        return .{
            .value_type = value_type,
            .value = value_token.string(),
        };
    }

    fn value_type_from_type_specifier_token(token: lex.Token) !ValueType {
        if (std.meta.stringToEnum(ValueType, token.string())) |value_type| {
            return value_type;
        } else {
            return ParserError.UnknownTypeSpecifier;
        }
    }
};

pub const ParserError = error{
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

// Test
const testing = std.testing;

test "Parse insert query" {
    var tokens = std.ArrayList(lex.Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_query = "insert [string 'name']->[string 'toothbrush'], [int 4], [float 3.0]-[string 'price']->[string 'bread'];";
    var lexer = lex.Lexer.init(insert_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try ExpressionNode.parse(testing.allocator, &query);
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
    var expression = try ExpressionNode.parse(testing.allocator, &query);
    defer expression.insert_connection.destory();
    try testing.expect(expression.insert_connection.connections.items.len == 2);
}
