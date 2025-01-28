const std = @import("std");

pub const LexingError = error{
    InvalidToken,
};

const Lexer = struct {
    source: []const u8,
    parent_state: State,
    current_state: State,
    buf: [4096]u8,
    current_token_len: u64,
    token_sequence: *std.ArrayList(Token),

    const State = enum {
        default,
        whitespace,
        keyword_or_identifier,
        connection_operator,
        numeric_value,
        string_value,
        node_block,
    };

    pub fn init(source: []const u8, token_sequence: *std.ArrayList(Token)) Lexer {
        return .{
            .source = source,
            .parent_state = .default,
            .current_state = .default,
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
                if (self.current_state == .string_value and self.source[pos - 1] == '\'') {
                    try self.on_state_change(pos, .whitespace);
                } else if (self.current_state != .string_value and self.current_state != .whitespace) {
                    try self.on_state_change(pos, .whitespace);
                } else if (self.current_state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                }
                return;
            },
            ',' => {
                if (self.current_state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else {
                    try self.on_state_change(pos, .default);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            '\'' => {
                if (self.current_state == .string_value) {
                    self.buf[self.current_token_len] = char;
                    self.current_token_len += 1;
                } else {
                    try self.on_state_change(pos, .string_value);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                }
            },
            '[' => {
                if (self.parent_state == .node_block) {
                    return LexingError.InvalidToken;
                }
                try self.on_state_change(pos, .node_block);
                self.buf[0] = char;
                self.current_token_len = 1;
            },
            ']' => {
                try self.on_state_change(pos, .node_block);
                self.buf[0] = char;
                self.current_token_len = 1;
                self.parent_state = .default;
            },
            '-' => {
                if (self.current_state == .connection_operator) {
                    return LexingError.InvalidToken;
                }
                try self.on_state_change(pos, .connection_operator);
                self.buf[0] = char;
                self.current_token_len = 1;
            },
            else => {
                if (is_numeric(char) and self.current_state != .numeric_value and self.current_state != .keyword_or_identifier and self.current_state != .string_value) {
                    try self.on_state_change(pos, .numeric_value);
                    self.buf[0] = char;
                    self.current_token_len = 1;
                } else if (is_alpha(char) and self.current_state != .keyword_or_identifier and self.current_state != .string_value) {
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
            try self.on_state_change(pos + 1, .default);
        }
    }

    inline fn on_state_change(self: *Lexer, pos: u64, new_state: State) !void {
        if (self.current_state != .whitespace and pos > 0) {
            try self.token_sequence.append(Token{
                .start = pos - self.current_token_len,
                .end = pos,
                .kind = try self.infer_token_kind(),
                .source = self.source,
            });
        }
        self.current_state = new_state;
        self.current_token_len = 0;
    }

    inline fn infer_token_kind(self: *Lexer) LexingError!Token.Kind {
        if (self.current_state == .string_value) {
            return Token.Kind.string_value;
        } else if (self.current_state == .numeric_value) {
            return Token.Kind.numeric_value;
        } else if (self.current_state == .connection_operator) {
            if (is_equal_string_ignore_case(self.buf[0..self.current_token_len], BUILTINS[12].name)) {
                return BUILTINS[10].kind;
            } else if (is_equal_string_ignore_case(self.buf[0..self.current_token_len], BUILTINS[13].name)) {
                return BUILTINS[11].kind;
            } else {
                return LexingError.InvalidToken;
            }
        } else if (self.current_state == .node_block) {
            if (is_equal_string_ignore_case(self.buf[0..self.current_token_len], BUILTINS[9].name)) {
                return BUILTINS[7].kind;
            } else if (is_equal_string_ignore_case(self.buf[0..self.current_token_len], BUILTINS[10].name)) {
                return BUILTINS[8].kind;
            } else {
                return LexingError.InvalidToken;
            }
        } else {
            for (BUILTINS) |builtin| {
                if (is_equal_string_ignore_case(builtin.name, self.buf[0..self.current_token_len])) {
                    return builtin.kind;
                }
            }
        }
        return Token.Kind.identifier;
    }
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
        comma,

        pre_label_connection_op,
        left_right_connection_op,

        boolean_type,
        smallint_type,
        int_type,
        bigint_type,
        smallserial_type,
        serial_type,
        bigserial_type,
        float_type,
        bigfloat_type,
        string_type,

        identifier,
        numeric_value,
        string_value,
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
    .{ .name = ",", .kind = Token.Kind.comma },
    .{ .name = "-", .kind = Token.Kind.pre_label_connection_op },
    .{ .name = "->", .kind = Token.Kind.left_right_connection_op },
    .{ .name = "boolean", .kind = Token.Kind.boolean_type },
    .{ .name = "smallint", .kind = Token.Kind.smallint_type },
    .{ .name = "int", .kind = Token.Kind.int_type },
    .{ .name = "bigint", .kind = Token.Kind.bigint_type },
    .{ .name = "smallserial", .kind = Token.Kind.smallserial_type },
    .{ .name = "serial", .kind = Token.Kind.serial_type },
    .{ .name = "bigserial", .kind = Token.Kind.bigserial_type },
    .{ .name = "float", .kind = Token.Kind.float_type },
    .{ .name = "bigfloat", .kind = Token.Kind.bigfloat_type },
    .{ .name = "string", .kind = Token.Kind.string_type },
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

    const insert_node_query = "insert [12.3 float], ['test' string], [23 int]";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 15);
}

test "Lexer#lex insert connection query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "insert connection [12.3 float]-['price' string]->['toothbrush' string]";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 16);
}

test "Lexer#lex delete connection query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "delete connection [12.3 float]-['price' string]->['toothbrush' string]";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 16);
}

test "Lexer#lex find query" {
    var tokens = std.ArrayList(Token).init(testing.allocator);
    defer tokens.deinit();

    const insert_node_query = "find x where [x]-['friend' string]->[y] and [x, y]-['type' string]->['person' string] and [y]-['name' string]->['John' string]";
    var lexer = Lexer.init(insert_node_query, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 45);
}
