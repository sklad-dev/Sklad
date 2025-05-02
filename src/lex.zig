const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");
const ApplicationError = @import("./constants.zig").ApplicationError;
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
        set_keyword,
        get_keyword,
        bool_value,
        numeric_value,
        string_value,
        comma,
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
    .{ .name = "set", .kind = Token.Kind.set_keyword },
    .{ .name = "get", .kind = Token.Kind.get_keyword },
    .{ .name = "true", .kind = Token.Kind.bool_value },
    .{ .name = "false", .kind = Token.Kind.bool_value },
    .{ .name = ",", .kind = Token.Kind.comma },
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
        keyword,
        numeric_value,
        string_value_start,
        string_value,
        string_value_end,
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
        } else {
            for (BUILTINS) |builtin| {
                if (isEqualStringIgnoreCase(builtin.name, self.buf[0..self.current_token_len])) {
                    return builtin.kind;
                }
            }
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

// Test
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

test "Lexer#lex set node query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const set_query1 = "set 'test' 4";
    var lexer = Lexer.init(set_query1, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 3);
    tokens.clearAndFree();

    const set_query2 = "set '0test1' 4, 'test1' 12.45, 'test2' -23456, 'test3' -12345.6789, 'falsy' false";
    lexer = Lexer.init(set_query2, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 15);
}

test "Lexer#lex get query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const get_query = "get 'test'";
    var l1 = Lexer.init(get_query, &tokens);
    try testing.expect(l1.lex() == 0);
    try testing.expect(tokens.items.len == 2);
    tokens.clearAndFree();
}
