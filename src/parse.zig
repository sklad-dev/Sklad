const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const lex = @import("./lex.zig");
const lexers = @import("./lexers.zig");
const utils = @import("./utils.zig");

const ApplicationError = @import("./constants.zig").ApplicationError;
const ExecuteTask = @import("./execute.zig").ExecuteTask;
const Task = @import("./task_queue.zig").Task;
const TypedBinaryData = @import("./data_types.zig").TypedBinaryData;
const ValueType = @import("./data_types.zig").ValueType;

const LexingError = lex.LexingError;
const Token = lex.Token;

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
        if (query.nextToken()) |token| {
            switch (token.kind) {
                .keyword => {
                    if (std.mem.eql(u8, token.string(), "set")) {
                        return Expression{
                            .set = try SetExpression.parse(allocator, query),
                        };
                    } else if (std.mem.eql(u8, token.string(), "get")) {
                        return Expression{
                            .get = try GetExpression.parse(allocator, query),
                        };
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

pub const SetExpression = struct {
    allocator: std.mem.Allocator,
    pairs: std.ArrayList(KeyValuePairNode),

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !SetExpression {
        var pairs = std.ArrayList(KeyValuePairNode).init(allocator);
        while (query.peakNextToken()) |token| {
            switch (token.kind) {
                .string_value, .numeric_value, .bool_value => try pairs.append(try KeyValuePairNode.parse(allocator, query)),
                .comma => {
                    _ = query.nextToken();
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
        if (query.peakNextToken()) |token| {
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
        const key_token = try query.expectToken(&[_]Token.Kind{ .string_value, .numeric_value, .bool_value });
        switch (key_token.kind) {
            .string_value => {
                return .{
                    .allocator = allocator,
                    .value = .{
                        .allocator = allocator,
                        .data_type = ValueType.string,
                        .data = try valueFromStr(allocator, .string, key_token.string()),
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
                            .data = try valueFromStr(allocator, .bigfloat, value_string),
                        },
                    };
                } else {
                    if (std.mem.startsWith(u8, value_string, "-")) {
                        if (valueFromStr(allocator, .smallint, value_string)) |value| {
                            return .{
                                .allocator = allocator,
                                .value = .{
                                    .allocator = allocator,
                                    .data_type = ValueType.smallint,
                                    .data = value,
                                },
                            };
                        } else |_| {
                            if (valueFromStr(allocator, .int, value_string)) |value| {
                                return .{
                                    .allocator = allocator,
                                    .value = .{
                                        .allocator = allocator,
                                        .data_type = ValueType.int,
                                        .data = value,
                                    },
                                };
                            } else |_| {
                                if (valueFromStr(allocator, .bigint, value_string)) |value| {
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
                        if (valueFromStr(allocator, .smallserial, value_string)) |value| {
                            return .{
                                .allocator = allocator,
                                .value = .{
                                    .allocator = allocator,
                                    .data_type = ValueType.smallserial,
                                    .data = value,
                                },
                            };
                        } else |_| {
                            if (valueFromStr(allocator, .serial, value_string)) |value| {
                                return .{
                                    .allocator = allocator,
                                    .value = .{
                                        .allocator = allocator,
                                        .data_type = ValueType.serial,
                                        .data = value,
                                    },
                                };
                            } else |_| {
                                if (valueFromStr(allocator, .bigserial, value_string)) |value| {
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
                        .data = try valueFromStr(allocator, .boolean, key_token.string()),
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
    tokens: *std.ArrayList(Token),
    current_pos: u64,

    pub fn init(allocator: std.mem.Allocator, tokens: *std.ArrayList(Token)) TokenizedQuery {
        return .{
            .allocator = allocator,
            .tokens = tokens,
            .current_pos = 0,
        };
    }

    pub fn nextToken(self: *TokenizedQuery) ?Token {
        if (self.current_pos > self.tokens.items.len - 1) {
            return null;
        }
        defer self.current_pos += 1;
        return self.tokens.items[self.current_pos];
    }

    pub fn peakNextToken(self: *TokenizedQuery) ?Token {
        if (self.current_pos > self.tokens.items.len - 1) {
            return null;
        }
        return self.tokens.items[self.current_pos];
    }

    pub fn expectToken(self: *TokenizedQuery, token_kinds: []const Token.Kind) !Token {
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

pub const QueryProcessingTask = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    query: []u8,

    pub fn init(allocator: std.mem.Allocator, query_size: u64, io_context: io.IO.IoContext) !QueryProcessingTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .query = try allocator.alloc(u8, query_size),
        };
    }

    pub fn task(self: *QueryProcessingTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
            .enqued_at = std.time.microTimestamp(),
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *QueryProcessingTask = @ptrCast(@alignCast(ptr));

        var tokens = std.ArrayList(Token).init(self.allocator);
        defer tokens.deinit();

        var lexer = lexers.kvLexer(self.query, &tokens);
        const lex_result = lexer.lex();
        if (lex_result > 0) {
            self.io_context.sendResponse(u64, LexingError, self.allocator, lex_result, LexingError.InvalidToken);
            std.posix.close(self.io_context.socket);
        }

        var tokenized_query = TokenizedQuery.init(self.allocator, &tokens);
        var expression = Expression.parse(self.allocator, &tokenized_query) catch |e| {
            std.log.err("Error! Query parsing failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.io_context.sendResponse(i8, ParserError, self.allocator, -1, ParserError.InvalidQuery);
            std.posix.close(self.io_context.socket);
            return;
        };

        const task_queue = global_context.getTaskQueue();
        var execute_task = task_queue.?.allocator.create(ExecuteTask) catch |e| {
            std.log.err("Error! Failed to allocate a parser task: {any}", .{e});
            self.handleParseError(&expression);
            return;
        };
        execute_task.* = ExecuteTask.init(
            task_queue.?.allocator,
            self.io_context,
            self.query,
            expression,
        ) catch |e| {
            std.log.err("Error! Failed to create a parser task: {any}", .{e});
            self.handleParseError(&expression);
            return;
        };

        global_context.getTaskQueue().?.enqueue(execute_task.task());
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *QueryProcessingTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }

    fn handleParseError(self: *QueryProcessingTask, expression: *Expression) void {
        switch (expression.*) {
            .set => expression.set.destroy(),
            .get => expression.get.destroy(),
        }
        self.io_context.sendResponse(i8, ApplicationError, self.allocator, -1, ApplicationError.InternalError);
        std.posix.close(self.io_context.socket);
    }
};

fn valueFromStr(allocator: std.mem.Allocator, value_type: ValueType, value_str: []const u8) ![]u8 {
    const tmp: []const u8 = switch (value_type) {
        .boolean => &try utils.toBytes(bool, (std.mem.eql(u8, value_str, "true"))),
        .smallint => &try utils.toBytes(i8, try std.fmt.parseInt(i8, value_str, 10)),
        .int => &try utils.toBytes(i32, try std.fmt.parseInt(i32, value_str, 10)),
        .bigint => &try utils.toBytes(i64, try std.fmt.parseInt(i64, value_str, 10)),
        .smallserial => &try utils.toBytes(u8, try std.fmt.parseInt(u8, value_str, 10)),
        .serial => &try utils.toBytes(u32, try std.fmt.parseInt(u32, value_str, 10)),
        .bigserial => &try utils.toBytes(u64, try std.fmt.parseInt(u64, value_str, 10)),
        .float => &try utils.toBytes(f32, try std.fmt.parseFloat(f32, value_str)),
        .bigfloat => &try utils.toBytes(f64, try std.fmt.parseFloat(f64, value_str)),
        .string => value_str,
    };
    const value = try allocator.alloc(u8, tmp.len);
    @memcpy(value, tmp);
    return value;
}

// Test
const testing = std.testing;

test "Parse set query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_query = "set 'test' 4, 'another test' 12.45, 'falsy' false";
    var lexer = lexers.kvLexer(insert_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.set.destroy();
    try testing.expect(expression.set.pairs.items.len == 3);
}

test "Parse get query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "get 'test'";
    var lexer = lexers.kvLexer(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.get.destroy();
    try testing.expect(std.mem.eql(u8, expression.get.key.value.data, "test"));
}
