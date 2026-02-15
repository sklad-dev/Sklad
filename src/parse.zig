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
    delete,
};

pub const Expression = union(ExpressionType) {
    set: SetExpression,
    get: GetExpression,
    delete: DeleteExpression,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !Expression {
        query.current_pos = 0;
        if (query.nextToken()) |token| {
            switch (token.kind) {
                .keyword => {
                    if (std.mem.eql(u8, token.string(), lexers.KV_BUILTINS[0].name)) {
                        return Expression{
                            .set = try SetExpression.parse(allocator, query),
                        };
                    } else if (std.mem.eql(u8, token.string(), lexers.KV_BUILTINS[1].name)) {
                        return Expression{
                            .get = try GetExpression.parse(allocator, query),
                        };
                    } else if (std.mem.eql(u8, token.string(), lexers.KV_BUILTINS[2].name)) {
                        return Expression{
                            .delete = try DeleteExpression.parse(allocator, query),
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
        var pairs = try std.ArrayList(KeyValuePairNode).initCapacity(allocator, 4);
        while (query.peakNextToken()) |token| {
            switch (token.kind) {
                .stringValue, .numericValue, .boolValue => try pairs.append(allocator, try KeyValuePairNode.parse(allocator, query)),
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
        self.pairs.deinit(self.allocator);
    }
};

pub const GetExpression = struct {
    allocator: std.mem.Allocator,
    parameter: GetParameterNode,
    batch_size: ?u64,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !GetExpression {
        var parameter: GetParameterNode = undefined;

        if (query.peakNextToken()) |token| {
            switch (token.kind) {
                .keyword => {
                    if (std.mem.eql(u8, token.string(), lexers.KV_BUILTINS[4].name)) {
                        _ = query.nextToken();
                        parameter = .{ .range = try RangeNode.parse(allocator, query) };
                    } else {
                        return ParserError.UnexpectedToken;
                    }
                },
                .stringValue, .numericValue, .boolValue => {
                    parameter = .{ .value = try ValueNode.parse(allocator, query) };
                },
                else => return ParserError.UnexpectedToken,
            }
        } else {
            return ParserError.InvalidQuery;
        }

        errdefer switch (parameter) {
            .range => |*r| r.deinit(),
            .value => |*v| v.deinit(),
        };

        var batch_size: ?u64 = null;
        if (query.peakNextToken()) |token| {
            if (token.kind == .keyword and std.mem.eql(u8, token.string(), lexers.KV_BUILTINS[5].name)) {
                _ = query.nextToken();
                const batch_token = try query.expectToken(&[_]Token.Kind{.numericValue});
                batch_size = std.fmt.parseInt(u64, batch_token.string(), 10) catch {
                    return ParserError.InvalidValue;
                };
            }
        }

        return .{
            .allocator = allocator,
            .parameter = parameter,
            .batch_size = batch_size,
        };
    }

    pub fn destroy(self: *GetExpression) void {
        switch (self.parameter) {
            .range => |*r| r.deinit(),
            .value => |*v| v.deinit(),
        }
    }
};

pub const DeleteExpression = struct {
    allocator: std.mem.Allocator,
    key: ValueNode,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !DeleteExpression {
        if (query.peakNextToken()) |token| {
            switch (token.kind) {
                .stringValue, .numericValue, .boolValue => return .{
                    .allocator = allocator,
                    .key = try ValueNode.parse(allocator, query),
                },
                else => return ParserError.UnexpectedToken,
            }
        }
        return ParserError.InvalidQuery;
    }

    pub fn destroy(self: *DeleteExpression) void {
        self.key.deinit();
    }
};

pub const KeyValuePairNode = struct {
    allocator: std.mem.Allocator,
    key: ValueNode,
    value: ValueNode,
    ttl: ?i64,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !KeyValuePairNode {
        const key = try ValueNode.parse(allocator, query);
        const value = try ValueNode.parse(allocator, query);
        var ttl: ?i64 = null;
        if (query.peakNextToken()) |token| {
            if (token.kind == .keyword and std.mem.eql(u8, token.string(), lexers.KV_BUILTINS[3].name)) {
                _ = query.nextToken();
                ttl = try parseTtl(query);
            }
        }

        return .{
            .allocator = allocator,
            .key = key,
            .value = value,
            .ttl = ttl,
        };
    }

    pub fn deinit(self: *const KeyValuePairNode) void {
        self.key.deinit();
        self.value.deinit();
    }

    fn parseTtl(query: *TokenizedQuery) !i64 {
        const token = try query.expectToken(&[_]Token.Kind{.stringValue});
        const time_str = token.string();

        if (time_str.len == 0) {
            return ParserError.InvalidValue;
        }

        if (time_str.len > 2 and std.mem.endsWith(u8, time_str, "ms")) {
            const num_str = time_str[0 .. time_str.len - 2];
            return std.fmt.parseInt(i64, num_str, 10) catch return ParserError.InvalidValue;
        }

        if (std.mem.endsWith(u8, time_str, "s")) {
            const num_str = time_str[0 .. time_str.len - 1];
            const seconds = std.fmt.parseInt(i64, num_str, 10) catch return ParserError.InvalidValue;
            return seconds * std.time.ms_per_s;
        }

        return std.fmt.parseInt(i64, time_str, 10) catch return ParserError.InvalidValue;
    }
};

pub const GetParameterType = enum {
    range,
    value,
};

pub const GetParameterNode = union(enum) {
    range: RangeNode,
    value: ValueNode,
};

pub const RangeNode = struct {
    allocator: std.mem.Allocator,
    start: ValueNode,
    end: ValueNode,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !RangeNode {
        const start = try ValueNode.parse(allocator, query);
        const end = try ValueNode.parse(allocator, query);

        if (start.value.data_type != end.value.data_type) {
            start.deinit();
            end.deinit();
            return ParserError.InvalidValue;
        }

        return .{
            .allocator = allocator,
            .start = start,
            .end = end,
        };
    }

    pub fn deinit(self: *const RangeNode) void {
        self.start.deinit();
        self.end.deinit();
    }
};

pub const ValueNode = struct {
    allocator: std.mem.Allocator,
    value: TypedBinaryData,

    pub fn parse(allocator: std.mem.Allocator, query: *TokenizedQuery) !ValueNode {
        const key_token = try query.expectToken(&[_]Token.Kind{ .stringValue, .numericValue, .boolValue });
        switch (key_token.kind) {
            .stringValue => {
                return .{
                    .allocator = allocator,
                    .value = .{
                        .allocator = allocator,
                        .data_type = ValueType.string,
                        .data = try valueFromStr(allocator, .string, key_token.string()),
                    },
                };
            },
            .numericValue => {
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
            .boolValue => {
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
    io_context: *io.IO.IoContext,
    query: []u8,

    pub fn init(allocator: std.mem.Allocator, query_size: u64, io_context: *io.IO.IoContext) !QueryProcessingTask {
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

        var tokens = std.ArrayList(Token).initCapacity(self.allocator, 16) catch |e| {
            std.log.err("Error! Failed to allocate a parser task: {any}", .{e});
            self.allocator.free(self.query);
            self.io_context.enqueueResponse(i8, ApplicationError, -1, ApplicationError.InternalError);
            return;
        };
        defer tokens.deinit(self.allocator);

        var lexer = lexers.kvLexer(self.allocator, self.query, &tokens);
        const lex_result = lexer.lex();
        if (lex_result > 0) {
            self.allocator.free(self.query);
            self.io_context.enqueueResponse(u64, LexingError, lex_result, LexingError.InvalidToken);
            return;
        }

        var tokenized_query = TokenizedQuery.init(self.allocator, &tokens);
        var expression = Expression.parse(self.allocator, &tokenized_query) catch |e| {
            std.log.err("Error! Query parsing failed: {any}, query: \"{s}\"", .{ e, self.query });
            self.allocator.free(self.query);
            self.io_context.enqueueResponse(i8, ParserError, -1, ParserError.InvalidQuery);
            return;
        };

        const task_queue = global_context.getTaskQueue();
        var execute_task = task_queue.?.allocator.create(ExecuteTask) catch |e| {
            std.log.err("Error! Failed to allocate a parser task: {any}", .{e});
            self.handleParseError(&expression);
            self.allocator.free(self.query);
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
            self.allocator.free(self.query);
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
            .delete => expression.delete.destroy(),
        }
        self.io_context.enqueueResponse(i8, ApplicationError, -1, ApplicationError.InternalError);
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
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_query = "set 'test' 4, 'another test' 12.45, 'falsy' false";
    var lexer = lexers.kvLexer(testing.allocator, insert_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.set.destroy();
    try testing.expect(expression.set.pairs.items.len == 3);
}

test "Parse set query with TTL in seconds" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const set_query_seconds = "set 'test' 1 expire '10s'";
    var lexer = lexers.kvLexer(testing.allocator, set_query_seconds, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.set.destroy();
    try testing.expect(expression.set.pairs.items.len == 1);
    try testing.expect(expression.set.pairs.items[0].ttl.? == 10 * std.time.ms_per_s);
    tokens.clearAndFree(testing.allocator);
}

test "Parse set query with TTL explicitly in milliseconds" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const set_query_ms = "set 'test' 4 expire '500ms'";
    var lexer = lexers.kvLexer(testing.allocator, set_query_ms, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.set.destroy();
    try testing.expect(expression.set.pairs.items[0].ttl.? == 500);
    tokens.clearAndFree(testing.allocator);
}

test "Parse set query with TTL implicitly in milliseconds" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const set_query_ms = "set 'test' 4 expire '987'";
    var lexer = lexers.kvLexer(testing.allocator, set_query_ms, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.set.destroy();
    try testing.expect(expression.set.pairs.items[0].ttl.? == 987);
    tokens.clearAndFree(testing.allocator);
}

test "Parse get query" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_node_query = "get 'test'";
    var lexer = lexers.kvLexer(testing.allocator, insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.get.destroy();
    try testing.expect(std.mem.eql(u8, expression.get.parameter.value.value.data, "test"));
}

test "Parse get range query" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_node_query = "get range 'a' 'z'";
    var lexer = lexers.kvLexer(testing.allocator, insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.get.destroy();
    try testing.expect(std.mem.eql(u8, expression.get.parameter.range.start.value.data, "a"));
    try testing.expect(std.mem.eql(u8, expression.get.parameter.range.end.value.data, "z"));
}

test "Parse get range query with mismatching value types" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_node_query = "get range 'a' 123";
    var lexer = lexers.kvLexer(testing.allocator, insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    const parse_result = Expression.parse(testing.allocator, &query);
    try testing.expect(parse_result == ParserError.InvalidValue);
}

test "Parse get range query with batch size" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_node_query = "get range 'a' 'z' batch 100";
    var lexer = lexers.kvLexer(testing.allocator, insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.get.destroy();
    try testing.expect(std.mem.eql(u8, expression.get.parameter.range.start.value.data, "a"));
    try testing.expect(std.mem.eql(u8, expression.get.parameter.range.end.value.data, "z"));
    try testing.expect(expression.get.batch_size.? == 100);
}

test "Parse get range query with incorrect batch size type" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_node_query = "get range 'a' 'z' batch 'invalid_size'";
    var lexer = lexers.kvLexer(testing.allocator, insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    const parse_result = Expression.parse(testing.allocator, &query);
    try testing.expect(parse_result == ParserError.UnexpectedToken);
}

test "Parse delete query" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const insert_node_query = "delete 'test'";
    var lexer = lexers.kvLexer(testing.allocator, insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);

    var query = TokenizedQuery.init(testing.allocator, &tokens);
    var expression = try Expression.parse(testing.allocator, &query);
    defer expression.delete.destroy();
    try testing.expect(std.mem.eql(u8, expression.delete.key.value.data, "test"));
}
