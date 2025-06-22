const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const ApplicationError = @import("./constants.zig").ApplicationError;
const Task = @import("./task_queue.zig").Task;

pub const LexingError = error{
    InvalidToken,
};

pub const Token = struct {
    start: u64,
    end: u64,
    kind: Kind,
    source: []const u8,

    pub const Kind = enum {
        keyword,
        identifier,
        bool_value,
        numeric_value,
        string_value,
        comma,
    };

    pub fn string(self: Token) []const u8 {
        return self.source[self.start..self.end];
    }
};

pub const Builtin = struct {
    name: []const u8,
    kind: Token.Kind,
};

var BOOLEAN_BUILTINS = [_]Builtin{
    .{ .name = "true", .kind = Token.Kind.bool_value },
    .{ .name = "false", .kind = Token.Kind.bool_value },
};

pub const Lexer = struct {
    builtins: []const Builtin,
    source: []const u8,
    state: State,
    buf: [4096]u8,
    current_token_len: u64,
    token_sequence: *std.ArrayList(Token),

    const State = enum {
        start,
        whitespace,
        comma,
        keyword,
        numeric_value,
        string_value_start,
        string_value,
        string_value_end,
        end,
    };

    pub fn init(builtins: []const Builtin, source: []const u8, token_sequence: *std.ArrayList(Token)) Lexer {
        return .{
            .builtins = builtins,
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
            self.updateState(c, i) catch |e| {
                std.log.err("Error! Lexing failed: {any}, query: \"{s}\", position: {d}", .{ e, self.source, i });
                return i;
            };
        }

        return 0;
    }

    fn updateState(self: *Lexer, char: u8, pos: u64) !void {
        switch (char) {
            ' ', '\n', '\t', '\r' => {
                if (self.state != .string_value and self.state != .whitespace) {
                    try self.onStateChange(pos, .whitespace);
                    self.current_token_len = 0;
                } else if (self.state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                }
                return;
            },
            ',' => {
                try self.handleSymbol(char, pos, .comma);
            },
            '\'' => {
                if (self.state == .string_value) {
                    try self.onStateChange(pos, .string_value_end);
                } else {
                    try self.onStateChange(pos, .string_value_start);
                }
            },
            else => {
                if ((isNumeric(char) or char == '-') and self.state != .numeric_value and self.state != .keyword and self.state != .string_value_start and self.state != .string_value) {
                    try self.onStateChange(pos, .numeric_value);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                } else if (self.state == .string_value_start) {
                    try self.onStateChange(pos, .string_value);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                } else if (isAlpha(char) and self.state != .keyword and self.state != .string_value_start and self.state != .string_value) {
                    try self.onStateChange(pos, .keyword);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                } else {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                }
            },
        }
        if (pos == self.source.len - 1) {
            try self.onStateChange(pos + 1, .end);
        }
    }

    inline fn handleSymbol(self: *Lexer, char: u8, pos: u64, new_state: State) !void {
        if (self.state == .string_value) {
            self.buf[self.current_token_len] = char;
            self.current_token_len += 1;
        } else {
            try self.onStateChange(pos, new_state);
            self.buf[0] = char;
            self.current_token_len = 1;
        }
    }

    inline fn onStateChange(self: *Lexer, pos: u64, new_state: State) !void {
        if (self.state != .whitespace and self.state != .string_value_end and self.state != .string_value_start and pos > 0) {
            try self.token_sequence.append(Token{
                .start = pos - self.current_token_len,
                .end = pos,
                .kind = try self.inferTokenKind(),
                .source = self.source,
            });
        }
        self.state = new_state;
        self.current_token_len = 0;
    }

    inline fn inferTokenKind(self: *Lexer) LexingError!Token.Kind {
        if (self.state == .string_value) {
            return Token.Kind.string_value;
        } else if (self.state == .numeric_value) {
            return Token.Kind.numeric_value;
        } else if (self.state == .comma) {
            return Token.Kind.comma;
        } else if (self.state == .keyword) {
            for (BOOLEAN_BUILTINS) |builtin| {
                if (isEqualStringIgnoreCase(builtin.name, self.buf[0..self.current_token_len])) {
                    return .bool_value;
                }
            }

            for (self.builtins) |builtin| {
                if (isEqualStringIgnoreCase(builtin.name, self.buf[0..self.current_token_len])) {
                    return .keyword;
                }
            }
            return .identifier;
        }
        return LexingError.InvalidToken;
    }
};

inline fn isAlpha(char: u8) bool {
    return (char >= 65 and char <= 90) or (char >= 95 and char <= 122);
}

inline fn isNumeric(char: u8) bool {
    return char >= 48 and char <= 57;
}

fn isEqualStringIgnoreCase(s1: []const u8, s2: []const u8) bool {
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

// Tests
const testing = std.testing;

test "#isEqualStringIgnoreCase" {
    try testing.expect(!isEqualStringIgnoreCase("test", "testtest"));
    try testing.expect(!isEqualStringIgnoreCase("test", ""));
    try testing.expect(isEqualStringIgnoreCase("", ""));
    try testing.expect(isEqualStringIgnoreCase("test", "test"));
    try testing.expect(isEqualStringIgnoreCase("TEST", "test"));
    try testing.expect(isEqualStringIgnoreCase("test", "TEST"));
    try testing.expect(isEqualStringIgnoreCase("TeSt", "tEsT"));
}

test "#isAlpha" {
    const alpha_string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (alpha_string) |c| {
        try testing.expect(isAlpha(c));
    }

    const not_alpha_string = " ,[]-<>0123456789";
    for (not_alpha_string) |c| {
        try testing.expect(!isAlpha(c));
    }
}

test "#isNumeric" {
    const numeric_string = "0123456789";
    for (numeric_string) |c| {
        try testing.expect(isNumeric(c));
    }

    const not_numeric_string = " ,[]-<>ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (not_numeric_string) |c| {
        try testing.expect(!isNumeric(c));
    }
}

test "kvLexer" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const test_builtins = [_]Builtin{
        .{ .name = "test", .kind = Token.Kind.keyword },
    };

    const query1 = "test";
    var lexer = Lexer.init(&test_builtins, query1, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 1);
    try testing.expect(tokens.items[0].kind == .keyword);
    tokens.clearAndFree();

    const query2 = "foo";
    lexer = Lexer.init(&test_builtins, query2, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 1);
    try testing.expect(tokens.items[0].kind == .identifier);
    tokens.clearAndFree();

    const query3 = "1234 -12.34";
    lexer = Lexer.init(&test_builtins, query3, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 2);
    try testing.expect(tokens.items[0].kind == .numeric_value);
    try testing.expect(tokens.items[1].kind == .numeric_value);
    tokens.clearAndFree();

    const query4 = "'test'";
    lexer = Lexer.init(&test_builtins, query4, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 1);
    try testing.expect(tokens.items[0].kind == .string_value);
    tokens.clearAndFree();

    const query5 = "true false";
    lexer = Lexer.init(&test_builtins, query5, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 2);
    try testing.expect(tokens.items[0].kind == .bool_value);
    try testing.expect(tokens.items[1].kind == .bool_value);
    tokens.clearAndFree();

    const query6 = "test 'test', 1234 foo bar, set key 'value', -12.23 -5, true";
    lexer = Lexer.init(&test_builtins, query6, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 15);
    tokens.clearAndFree();
}
