const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const Task = @import("./task_queue.zig").Task;
const ParserTask = @import("./parse.zig").ParserTask;

pub const LexingError = error{
    InvalidToken,
};

pub const Token = struct {
    start: u64,
    end: u64,
    kind: Kind,
    source: []const u8,

    pub const Kind = enum {
        insert_keyword,
        connection_keyword,
        find_keyword,
        delete_keyword,
        where_keyword,
        and_keyword,
        or_keyword,
        true_keyword,
        false_keyword,

        left_square_bracket,
        right_square_bracket,
        pre_label_connection_op,
        left_right_connection_op,

        identifier,
        type_specifier,
        numeric_value,
        string_value,

        comma,
        semicolon,
    };

    pub fn string(self: Token) []const u8 {
        return self.source[self.start..self.end];
    }
};

const Builtin = struct {
    name: []const u8,
    kind: Token.Kind,
};

var BUILTINS = [_]Builtin{
    .{ .name = "insert", .kind = Token.Kind.insert_keyword },
    .{ .name = "connection", .kind = Token.Kind.connection_keyword },
    .{ .name = "find", .kind = Token.Kind.find_keyword },
    .{ .name = "delete", .kind = Token.Kind.delete_keyword },
    .{ .name = "where", .kind = Token.Kind.where_keyword },
    .{ .name = "and", .kind = Token.Kind.and_keyword },
    .{ .name = "or", .kind = Token.Kind.or_keyword },
    .{ .name = "true", .kind = Token.Kind.true_keyword },
    .{ .name = "false", .kind = Token.Kind.false_keyword },
    .{ .name = "[", .kind = Token.Kind.left_square_bracket },
    .{ .name = "]", .kind = Token.Kind.right_square_bracket },
    .{ .name = "-", .kind = Token.Kind.pre_label_connection_op },
    .{ .name = "->", .kind = Token.Kind.left_right_connection_op },
    .{ .name = "boolean", .kind = Token.Kind.type_specifier },
    .{ .name = "smallint", .kind = Token.Kind.type_specifier },
    .{ .name = "int", .kind = Token.Kind.type_specifier },
    .{ .name = "bigint", .kind = Token.Kind.type_specifier },
    .{ .name = "smallserial", .kind = Token.Kind.type_specifier },
    .{ .name = "serial", .kind = Token.Kind.type_specifier },
    .{ .name = "bigserial", .kind = Token.Kind.type_specifier },
    .{ .name = "float", .kind = Token.Kind.type_specifier },
    .{ .name = "bigfloat", .kind = Token.Kind.type_specifier },
    .{ .name = "string", .kind = Token.Kind.type_specifier },
    .{ .name = ",", .kind = Token.Kind.comma },
    .{ .name = ";", .kind = Token.Kind.semicolon },
};

pub const Lexer = struct {
    source: []const u8,
    state: State,
    buf: [4096]u8,
    current_token_len: u64,
    token_sequence: *std.ArrayList(Token),

    const State = enum {
        start,
        whitespace,
        comma,
        keyword_or_identifier,
        connection_operator,
        numeric_value,
        string_value,
        string_value_end,
        node_block_start,
        node_block_end,
        end,
    };

    pub fn init(source: []const u8, token_sequence: *std.ArrayList(Token)) Lexer {
        return .{
            .source = source,
            .state = .start,
            .buf = [_]u8{0} ** 4096,
            .current_token_len = 0,
            .token_sequence = token_sequence,
        };
    }

    pub fn lex(self: *Lexer) u64 {
        if (self.source.len == 0) return 0;

        for (self.source, 0..) |c, i| {
            self.update_state(c, i) catch |e| {
                std.log.err("Error! Lexing failed: {any}, query: \"{s}\", position: {d}", .{ e, self.source, i });
                return i;
            };
        }

        return 0;
    }

    fn update_state(self: *Lexer, char: u8, pos: u64) !void {
        switch (char) {
            ' ', '\n', '\t', '\r' => {
                if (self.state != .string_value and self.state != .whitespace) {
                    try self.on_state_change(pos, .whitespace);
                } else if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                }
                return;
            },
            ',' => {
                if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else {
                    try self.on_state_change(pos, .comma);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            '\'' => {
                if (self.state == .string_value) {
                    try self.on_state_change(pos, .string_value_end);
                    self.current_token_len = 1;
                } else {
                    try self.on_state_change(pos, .string_value);
                }
            },
            '[' => {
                if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else {
                    try self.on_state_change(pos, .node_block_start);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            ']' => {
                if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else {
                    try self.on_state_change(pos, .node_block_end);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            '-' => {
                if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else if (self.state == .connection_operator) {
                    return LexingError.InvalidToken;
                } else {
                    try self.on_state_change(pos, .connection_operator);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            ';' => {
                if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else {
                    try self.on_state_change(pos, .end);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            else => {
                if (is_numeric(char) and self.state != .numeric_value and self.state != .keyword_or_identifier and self.state != .string_value) {
                    try self.on_state_change(pos, .numeric_value);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                } else if (is_alpha(char) and self.state != .keyword_or_identifier and self.state != .string_value) {
                    try self.on_state_change(pos, .keyword_or_identifier);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                } else {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                }
            },
        }
        if (pos == self.source.len - 1) {
            try self.on_state_change(pos + 1, .start);
        }
    }

    inline fn on_state_change(self: *Lexer, pos: u64, new_state: State) !void {
        if (self.state != .whitespace and self.state != .string_value_end and pos > 0) {
            try self.token_sequence.append(Token{
                .start = pos - self.current_token_len,
                .end = pos,
                .kind = try self.infer_token_kind(),
                .source = self.source,
            });
        }
        self.state = new_state;
        self.current_token_len = 0;
    }

    inline fn infer_token_kind(self: *Lexer) LexingError!Token.Kind {
        if (self.state == .string_value) {
            return Token.Kind.string_value;
        } else if (self.state == .numeric_value) {
            return Token.Kind.numeric_value;
        } else if (self.state == .node_block_start) {
            return Token.Kind.left_square_bracket;
        } else if (self.state == .node_block_end) {
            return Token.Kind.right_square_bracket;
        } else {
            for (BUILTINS) |builtin| {
                if (is_equal_string_ignore_case(builtin.name, self.buf[0..self.current_token_len])) {
                    return builtin.kind;
                }
            }
            if (self.state == .keyword_or_identifier) {
                return Token.Kind.identifier;
            }
        }
        return LexingError.InvalidToken;
    }
};

inline fn is_alpha(char: u8) bool {
    return (char >= 65 and char <= 90) or (char >= 95 and char <= 122);
}

inline fn is_numeric(char: u8) bool {
    return char >= 48 and char <= 57;
}

fn is_equal_string_ignore_case(s1: []const u8, s2: []const u8) bool {
    if (s1.len != s2.len) return false;

    for (s1, s2) |s1_char, s2_char| {
        var s1_char_tmp = s1_char;
        if (s1_char_tmp >= 97 and s1_char_tmp <= 122) {
            s1_char_tmp -= 32;
        }

        var s2_char_tmp = s2_char;
        if (s2_char_tmp >= 97 and s2_char_tmp <= 122) {
            s2_char_tmp -= 32;
        }

        if (s1_char_tmp != s2_char_tmp) return false;
    }

    return true;
}

pub const LexerTask = struct {
    allocator: std.mem.Allocator,
    io_context: io.IO.IoContext,
    query: []u8,
    tokens: *std.ArrayList(Token),

    pub fn init(allocator: std.mem.Allocator, query_size: u64, io_context: io.IO.IoContext) !LexerTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .query = try allocator.alloc(u8, query_size),
            .tokens = try allocator.create(std.ArrayList(Token)),
        };
    }

    pub fn task(self: *LexerTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *LexerTask = @ptrCast(@alignCast(ptr));
        errdefer std.posix.close(self.io_context.socket);

        var lexer = Lexer.init(self.query, self.tokens);
        const result = lexer.lex();
        if (result > 0) {
            self.io_context.send_response(u64, LexingError, self.allocator, result, LexingError.InvalidToken);
        } else {
            const task_queue = global_context.get_task_queue();
            var parser_task = task_queue.?.allocator.create(ParserTask) catch |e| {
                std.log.err("Error! Failed to allocate a parser task: {any}", .{e});
                return;
            };
            parser_task.* = ParserTask.init(
                task_queue.?.allocator,
                self.io_context,
                self.query,
                self.tokens,
            ) catch |e| {
                std.log.err("Error! Failed to create a parser task: {any}", .{e});
                return;
            };

            global_context.get_task_queue().?.enqueue(parser_task.task());
        }
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *LexerTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

// Test
const testing = std.testing;

test "#is_equal_string_ignore_case" {
    try testing.expect(!is_equal_string_ignore_case("test", "testtest"));
    try testing.expect(!is_equal_string_ignore_case("test", ""));
    try testing.expect(is_equal_string_ignore_case("", ""));
    try testing.expect(is_equal_string_ignore_case("test", "test"));
    try testing.expect(is_equal_string_ignore_case("TEST", "test"));
    try testing.expect(is_equal_string_ignore_case("test", "TEST"));
    try testing.expect(is_equal_string_ignore_case("TeSt", "tEsT"));
}

test "#is_alpha" {
    const alpha_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (alpha_string) |c| {
        try testing.expect(is_alpha(c));
    }

    const not_alpha_string = " ,[]-<>0123456789";
    for (not_alpha_string) |c| {
        try testing.expect(!is_alpha(c));
    }
}

test "#is_numeric" {
    const numeric_string = "0123456789";
    for (numeric_string) |c| {
        try testing.expect(is_numeric(c));
    }

    const not_numeric_string = " ,[]-<>ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (not_numeric_string) |c| {
        try testing.expect(!is_numeric(c));
    }
}

test "Lexer#lex insert node query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "insert [float 12.3], [string 'test'], [int 23];";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 16);
}

test "Lexer#lex insert connection query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "insert connection [float 12.3]-[string 'price']->[string 'toothbrush'], [float 3.0]-[string 'price']->[string 'bread'];";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 32);
}

test "Lexer#lex delete connection query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "delete connection [float 12.3]-[string 'price']->[string 'toothbrush'];";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 17);
}

test "Lexer#lex find query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "find x where [x]-[string 'friend']->[y] and [x, y]-[string 'type']->[string 'person'] and [y]-[string 'name']->[string 'John'];";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 46);
}
