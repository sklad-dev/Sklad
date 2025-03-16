const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const lex = @import("./lex.zig");
const utils = @import("./utils.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const ValueType = @import("./data_types.zig").ValueType;
const TypedBinaryData = @import("./data_types.zig").TypedBinaryData;
const Task = @import("./task_queue.zig").Task;
const ExecuteTask = @import("./execute.zig").ExecuteTask;

pub const ParserError = error{
    InvalidQuery,
    InvalidValue,
    UnexpectedToken,
    UnexpectedEndOfQuery,
};

pub const ExpressionType = enum {
    set,
    get,
};

pub const Expression = union(ExpressionType) {
    set: SetExpression,
    get: GetExpression,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !Expression {
        query.current_pos = 0;
        if (query.next_token()) |token| {
            switch (token.kind) {
                .set_keyword => return Expression{
                    .set = try SetExpression.parse(allocator, query),
                },
                .get_keyword => return Expression{
                    .get = try GetExpression.parse(allocator, query),
                },
                else => return ParserError.UnexpectedToken,
            }
        } else {
            return ParserError.UnexpectedToken;
        }
    }
};

pub const SetExpression = struct {
    allocator: std.mem.Allocator,
    pairs: std.ArrayList(KeyValuePairNode),

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !SetExpression {
        var pairs = std.ArrayList(KeyValuePairNode).init(allocator);
        while (query.peak_next_token()) |token| {
            switch (token.kind) {
                .string_value, .numeric_value, .bool_value => try pairs.append(try KeyValuePairNode.parse(allocator, query)),
                .comma => {
                    _ = query.next_token();
                    continue;
                },
                else => return ParserError.UnexpectedToken,
            }
        }
        return .{
            .allocator = allocator,
            .pairs = pairs,
        };
    }

    pub fn destroy(self: *SetExpression) void {
        for (self.pairs.items) |pair| {
            pair.deinit();
        }
        self.pairs.deinit();
    }
};

pub const GetExpression = struct {
    allocator: std.mem.Allocator,
    key: ValueNode,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !GetExpression {
        if (query.peak_next_token()) |token| {
            switch (token.kind) {
                .string_value, .numeric_value, .bool_value => return .{
                    .allocator = allocator,
                    .key = try ValueNode.parse(allocator, query),
                },
                else => return ParserError.UnexpectedToken,
            }
        }
        return ParserError.InvalidQuery;
    }

    pub fn destroy(self: *GetExpression) void {
        self.key.deinit();
    }
};

pub const KeyValuePairNode = struct {
    allocator: std.mem.Allocator,
    key: ValueNode,
    value: ValueNode,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !KeyValuePairNode {
        const key = try ValueNode.parse(allocator, query);
        const value = try ValueNode.parse(allocator, query);
        return .{
            .allocator = allocator,
            .key = key,
            .value = value,
        };
    }

    pub fn deinit(self: *const KeyValuePairNode) void {
        self.key.deinit();
        self.value.deinit();
    }
};

pub const ValueNode = struct {
    allocator: std.mem.Allocator,
    value: TypedBinaryData,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !ValueNode {
        const key_token = try query.expect_token(&[_]lex.Token.Kind{ .string_value, .numeric_value, .bool_value });
        switch (key_token.kind) {
            .string_value => {
                return .{
                    .allocator = allocator,
                    .value = .{
                        .allocator = allocator,
                        .data_type = ValueType.string,
                        .data = try value_from_str(allocator, .string, key_token.string()),
                    },
                };
            },
            .numeric_value => {
                const value_string = key_token.string();
                if (std.mem.containsAtLeast(u8, value_string, 1, ".")) {
                    return .{
                        .allocator = allocator,
                        .value = .{
                            .allocator = allocator,
                            .data_type = ValueType.bigfloat,
                            .data = try value_from_str(allocator, .bigfloat, value_string),
                        },
                    };
                } else {
                    if (std.mem.startsWith(u8, value_string, "-")) {
                        if (value_from_str(allocator, .smallint, value_string)) |value| {
                            return .{
                                .allocator = allocator,
                                .value = .{
                                    .allocator = allocator,
                                    .data_type = ValueType.smallint,
                                    .data = value,
                                },
                            };
                        } else |_| {
                            if (value_from_str(allocator, .int, value_string)) |value| {
                                return .{
                                    .allocator = allocator,
                                    .value = .{
                                        .allocator = allocator,
                                        .data_type = ValueType.int,
                                        .data = value,
                                    },
                                };
                            } else |_| {
                                if (value_from_str(allocator, .bigint, value_string)) |value| {
                                    return .{
                                        .allocator = allocator,
                                        .value = .{
                                            .allocator = allocator,
                                            .data_type = ValueType.bigint,
                                            .data = value,
                                        },
                                    };
                                } else |_| {
                                    return ParserError.InvalidValue;
                                }
                            }
                        }
                    } else {
                        if (value_from_str(allocator, .smallserial, value_string)) |value| {
                            return .{
                                .allocator = allocator,
                                .value = .{
                                    .allocator = allocator,
                                    .data_type = ValueType.smallserial,
                                    .data = value,
                                },
                            };
                        } else |_| {
                            if (value_from_str(allocator, .serial, value_string)) |value| {
                                return .{
                                    .allocator = allocator,
                                    .value = .{
                                        .allocator = allocator,
                                        .data_type = ValueType.serial,
                                        .data = value,
                                    },
                                };
                            } else |_| {
                                if (value_from_str(allocator, .bigserial, value_string)) |value| {
                                    return .{
                                        .allocator = allocator,
                                        .value = .{
                                            .allocator = allocator,
                                            .data_type = ValueType.bigserial,
                                            .data = value,
                                        },
                                    };
                                } else |_| {
                                    return ParserError.InvalidValue;
                                }
                            }
                        }
                    }
                }
            },
            .bool_value => {
                return .{
                    .allocator = allocator,
                    .value = .{
                        .allocator = allocator,
                        .data_type = ValueType.boolean,
                        .data = try value_from_str(allocator, .boolean, key_token.string()),
                    },
                };
            },
            else => unreachable,
        }
    }

    pub fn deinit(self: *const ValueNode) void {
        self.allocator.free(self.value.data);
    }
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
        if (self.current_pos > self.tokens.items.len - 1) {
            return null;
        }
        defer self.current_pos += 1;
        return self.tokens.items[self.current_pos];
    }

    pub fn peak_next_token(self: *TokenizedQuery) ?lex.Token {
        if (self.current_pos > self.tokens.items.len - 1) {
            return null;
        }
        return self.tokens.items[self.current_pos];
    }

    pub fn expect_token(self: *TokenizedQuery, token_kinds: []const lex.Token.Kind) !lex.Token {
        if (self.current_pos > self.tokens.items.len - 1) {
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

        var expression = Expression.parse(self.allocator, &self.tokenized_query) catch |e| {
            std.log.err("Error! Query parsing failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.send_response(i8, ParserError, self.allocator, -1, ParserError.InvalidQuery);
            std.posix.close(self.io_context.socket);
            return;
        };

        const task_queue = global_context.get_task_queue();
        var execute_task = task_queue.?.allocator.create(ExecuteTask) catch |e| {
            std.log.err("Error! Failed to allocate a parser task: {any}", .{e});
            self.handle_error(&expression);
            return;
        };
        execute_task.* = ExecuteTask.init(
            task_queue.?.allocator,
            self.io_context,
            self.query,
            expression,
        ) catch |e| {
            std.log.err("Error! Failed to create a parser task: {any}", .{e});
            self.handle_error(&expression);
            return;
        };

        global_context.get_task_queue().?.enqueue(execute_task.task());
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *ParserTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self.tokenized_query.tokens);
        allocator.destroy(self);
    }

    fn handle_error(self: *ParserTask, expression: *Expression) void {
        switch (expression.*) {
            .set => expression.set.destroy(),
            .get => expression.get.destroy(),
        }
        self.io_context.send_response(i8, ApplicationError, self.allocator, -1, ApplicationError.InternalError);
        std.posix.close(self.io_context.socket);
    }
};

fn value_from_str(allocator: std.mem.Allocator, value_type: ValueType, value_str: []const u8) ![]u8 {
    const tmp: []const u8 = switch (value_type) {
        .boolean => &try utils.to_bytes(bool, (std.mem.eql(u8, value_str, "true"))),
        .smallint => &try utils.to_bytes(i8, try std.fmt.parseInt(i8, value_str, 10)),
        .int => &try utils.to_bytes(i32, try std.fmt.parseInt(i32, value_str, 10)),
        .bigint => &try utils.to_bytes(i64, try std.fmt.parseInt(i64, value_str, 10)),
        .smallserial => &try utils.to_bytes(u8, try std.fmt.parseInt(u8, value_str, 10)),
        .serial => &try utils.to_bytes(u32, try std.fmt.parseInt(u32, value_str, 10)),
        .bigserial => &try utils.to_bytes(u64, try std.fmt.parseInt(u64, value_str, 10)),
        .float => &try utils.to_bytes(f32, try std.fmt.parseFloat(f32, value_str)),
        .bigfloat => &try utils.to_bytes(f64, try std.fmt.parseFloat(f64, value_str)),
        .string => value_str,
    };
    const value = try allocator.alloc(u8, tmp.len);
    @memcpy(value, tmp);
    return value;
}

// Test
const testing = std.testing;

test "Parse set query" {
    var tokens = std.ArrayList(lex.Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_query = "set 'test' 4, 'another test' 12.45, 'falsy' false";
    var lexer = lex.Lexer.init(insert_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.set.destroy();
    try testing.expect(expression.set.pairs.items.len == 3);
}

test "Parse get query" {
    var tokens = std.ArrayList(lex.Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "get 'test'";
    var lexer = lex.Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.get.destroy();
    try testing.expect(std.mem.eql(u8, expression.get.key.value.data, "test"));
}
