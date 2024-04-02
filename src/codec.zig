/// https://kafka.apache.org/protocol#protocol_types
const std = @import("std");

/// Decodes kafka protocol types from bytes read from the wire
pub const Decoder = struct {
    data: []const u8,
    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{ .data = data };
    }

    fn read(self: *Self, n: usize) ![]const u8 {
        if (n > self.data.len) {
            return error.EOF;
        } else {
            var slice = self.data[0..n];
            self.data = self.data[n..];
            return slice;
        }
    }

    fn readBool(self: *Self) !bool {
        return (try self.readI8()) != 0;
    }

    fn readI8(self: *Self) !i8 {
        return @intCast((try self.read(1))[0]);
    }

    fn readI16(self: *Self) !i16 {
        return std.mem.readIntBig(i16, (try self.read(2))[0..2]);
    }

    fn readI32(self: *Self) !i32 {
        return std.mem.readIntBig(i32, (try self.read(4))[0..4]);
    }

    fn readI64(self: *Self) !i64 {
        return std.mem.readIntBig(i64, (try self.read(8))[0..8]);
    }

    fn readF64(self: *Self) !f64 {
        return @bitCast(@constCast((try self.read(8))[0..8]).*);
    }

    fn readStr(self: *Self) ![]const u8 {
        const len = try self.readI16();
        if (len <= 0) {
            return "";
        }
        return self.read(@intCast(len));
    }

    fn readBytes(self: *Self) ![]const u8 {
        const len = try self.readI32();
        if (len <= 0) {
            return self.data[0..0];
        }
        return self.read(@intCast(len));
    }

    fn readArrayLen(self: *Self) !usize {
        const len = try self.read32();
        return if (len < 0) 0 else @intCast(len);
    }

    fn isEmpty(self: *Self) bool {
        return self.data.len < 1;
    }
};

/// Encodes kafka protocol types to a target writer
pub fn Encoder(comptime Writer: type) type {
    return struct {
        writer: Writer,
        const Self = @This();
        pub fn init(writer: Writer) Self {
            return .{ .writer = writer };
        }

        fn encodeByte(self: *Self, b: u8) !void {
            try self.writeByte(b);
        }

        pub fn encodeBool(self: *Self, b: bool) !void {
            try self.encodeI8(if (b) 1 else 0);
        }

        pub fn encodeI8(self: *Self, i: i8) !void {
            try self.writer.writeInt(i8, i, .Big);
        }

        pub fn encodeI32(self: *Self, i: i32) !void {
            try self.writer.writeInt(i32, i, .Big);
        }

        pub fn encodeI64(self: *Self, i: i64) !void {
            try self.writer.writeInt(i64, i, .Big);
        }

        pub fn encodeF64(self: *Self, f: f64) !void {
            try self.writer.writeAll(&@as([8]u8, @bitCast(f)));
        }

        pub fn encodeStr(self: *Self, s: []const u8) !void {
            const len: i16 = @intCast(s.len);
            try self.writer.writeInt(i16, len, .Big);
            try self.writer.writeAll(s);
        }

        pub fn encodeBytes(self: *Self, s: []const u8) !void {
            const len: i32 = @intCast(s.len);
            try self.writer.writeInt(i32, len, .Big);
            try self.writer.writeAll(s);
        }
    };
}

test "round trip" {
    const allocator = std.testing.allocator;
    var buffer = std.ArrayList(u8).init(allocator);

    var encoder = Encoder(std.ArrayList(u8).Writer).init(buffer.writer());

    try encoder.encodeBool(true);
    try encoder.encodeBool(false);
    try encoder.encodeBytes("rand");
    try encoder.encodeStr("str");
    try encoder.encodeI32(32);
    try encoder.encodeI64(64);
    try encoder.encodeI8(8);
    try encoder.encodeF64(5.123);

    var bytes = try buffer.toOwnedSlice();
    defer allocator.free(bytes);

    var decoder = Decoder.init(bytes);

    try std.testing.expectEqual(true, try decoder.readBool());
    try std.testing.expectEqual(false, try decoder.readBool());
    try std.testing.expectEqualSlices(u8, "rand", try decoder.readBytes());
    try std.testing.expectEqualSlices(u8, "str", try decoder.readStr());
    try std.testing.expectEqual(try decoder.readI32(), 32);
    try std.testing.expectEqual(try decoder.readI64(), 64);
    try std.testing.expectEqual(try decoder.readI8(), 8);
    try std.testing.expectEqual(try decoder.readF64(), 5.123);
    try std.testing.expect(decoder.isEmpty());
}
