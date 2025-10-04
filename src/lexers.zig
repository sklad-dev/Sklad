const std = @import("std");
const lex = @import("./lex.zig");
const Builtin = lex.Builtin;
const Lexer = lex.Lexer;
const Token = lex.Token;

pub const KV_BUILTINS = [_]Builtin{
    .{ .name = "set", .kind = Token.Kind.keyword },
    .{ .name = "get", .kind = Token.Kind.keyword },
};

pub inline fn kvLexer(allocator: std.mem.Allocator, source: []const u8, token_sequence: *std.ArrayList(Token)) lex.Lexer {
    return Lexer.init(allocator, &KV_BUILTINS, source, token_sequence);
}

// Tests
const testing = std.testing;

test "kvLexer set node query" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const set_query1 = "set 'test' 4";
    var lexer = kvLexer(testing.allocator, set_query1, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 3);
    tokens.clearAndFree(testing.allocator);

    const set_query2 = "set '0test1' 4, 'test1' 12.45, 'test2' -23456, 'test3' -12345.6789, 'falsy' false";
    lexer = kvLexer(testing.allocator, set_query2, &tokens);
    try testing.expect(lexer.lex() == 0);
    try testing.expect(tokens.items.len == 15);
}

test "kvLexer get query" {
    var tokens = try std.ArrayList(Token).initCapacity(testing.allocator, 16);
    defer tokens.deinit(testing.allocator);

    const get_query = "get 'test'";
    var l1 = kvLexer(testing.allocator, get_query, &tokens);
    try testing.expect(l1.lex() == 0);
    try testing.expect(tokens.items.len == 2);
    tokens.clearAndFree(testing.allocator);
}
