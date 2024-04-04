/// https://kafka.apache.org/protocol#protocol_types
const std = @import("std");

/// Decodes kafka protocol types from bytes read from the wire
pub const Reader = struct {
    data: []const u8,
    allocator: std.mem.Allocator,
    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, data: []const u8) Self {
        return .{ .allocator = allocator, .data = data };
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

    pub fn readBool(self: *Self) !bool {
        return (try self.readI8()) != 0;
    }

    pub fn readI8(self: *Self) !i8 {
        return @intCast((try self.read(1))[0]);
    }

    pub fn readI16(self: *Self) !i16 {
        return std.mem.readIntBig(i16, (try self.read(2))[0..2]);
    }

    pub fn readI32(self: *Self) !i32 {
        return std.mem.readIntBig(i32, (try self.read(4))[0..4]);
    }

    pub fn readI64(self: *Self) !i64 {
        return std.mem.readIntBig(i64, (try self.read(8))[0..8]);
    }

    pub fn readF64(self: *Self) !f64 {
        return @bitCast(@constCast((try self.read(8))[0..8]).*);
    }

    pub fn readStr(self: *Self) ![]const u8 {
        const len = try self.readI16();
        return if (len > 0) self.read(@intCast(len)) else "";
    }

    pub fn readBytes(self: *Self) ![]const u8 {
        const len = try self.readI32();
        return if (len > 0) self.read(@intCast(len)) else self.data[0..0];
    }

    pub fn readArrayLen(self: *Self) !i32 {
        return @max(try self.readI32(), 0);
    }

    fn isEmpty(self: *Self) bool {
        return self.data.len < 1;
    }

    pub fn readType(self: *Self, comptime T: type) !T {
        const info = @typeInfo(T);
        switch (info) {
            .Struct => |s| {
                var parsed: T = undefined;
                // todo: provide hook for custom struct reading
                inline for (s.fields) |field| {
                    @field(parsed, field.name) = try self.readType(field.type);
                }
                return parsed;
            },
            .Int => |i| {
                switch (i.bits) {
                    8 => return self.readI8(),
                    16 => return self.readI16(),
                    32 => return self.readI32(),
                    64 => return self.readI64(),
                    else => @compileError("int with these bits not supported"),
                }
            },
            .Float => |f| {
                switch (f.bits) {
                    64 => return self.readF64(),
                    else => @compileError("float with these bits not supported"),
                }
            },
            .Bool => {
                return self.readBool();
            },
            .Pointer => |p| {
                // fixme: how do we distinguish between string and bytes?
                if (p.child == u8) {
                    return self.readStr();
                }
                const len = try self.readI32();

                // var buf = try std.ArrayList(p.child).initCapacity(self.allocator, @intCast(len));
                // defer buf.deinit();
                // try buf.resize(@intCast(len));
                // var slice = try buf.toOwnedSlice();
                // for (0..@intCast(len)) |i| {
                //     slice[i] = try self.readType(p.child);
                // }
                // return slice;
                std.debug.print("dont yet now how to read slice of {any} {any}s\n", .{ len, @typeName(T) });
                // @compileLog("note: reading slices of type " ++ @typeName(p.child) ++ "is not yet supported");
                return &[_]p.child{};
                // todo: how to read into a slice?
                //const len = try self.readI16();
                //return if (len > 0) self.read(@intCast(len)) else "";
            },
            else => |otherwise| {
                std.debug.print("unable to parse type {any}", .{otherwise});
                @compileError("supported type " ++ @typeName(T));
            },
        }
    }
};

pub fn packU32(u: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    var bufWriter = std.io.fixedBufferStream(&buf);
    bufWriter.writer().writeInt(u32, u, .Big) catch unreachable;
    return buf;
}

/// Encodes kafka protocol types to a target writer
pub fn Writer(comptime WrappedWriter: type) type {
    return struct {
        writer: WrappedWriter,
        const Self = @This();
        pub fn init(writer: WrappedWriter) Self {
            return .{ .writer = writer };
        }

        fn writeByte(self: *Self, b: u8) !void {
            try self.writeByte(b);
        }

        pub fn writeBool(self: *Self, b: bool) !void {
            try self.writeI8(if (b) 1 else 0);
        }

        pub fn writeI8(self: *Self, i: i8) !void {
            try self.writer.writeInt(i8, i, .Big);
        }

        pub fn writeI16(self: *Self, i: i16) !void {
            try self.writer.writeInt(i16, i, .Big);
        }

        pub fn writeI32(self: *Self, i: i32) !void {
            try self.writer.writeInt(i32, i, .Big);
        }

        pub fn writeI64(self: *Self, i: i64) !void {
            try self.writer.writeInt(i64, i, .Big);
        }

        pub fn writeF64(self: *Self, f: f64) !void {
            try self.writer.writeAll(&@as([8]u8, @bitCast(f)));
        }

        pub fn writeStr(self: *Self, s: []const u8) !void {
            const len: i16 = @intCast(s.len);
            try self.writer.writeInt(i16, len, .Big);
            try self.writer.writeAll(s);
        }

        pub fn writeBytes(self: *Self, s: []const u8) !void {
            const len: i32 = @intCast(s.len);
            try self.writer.writeInt(i32, len, .Big);
            try self.writer.writeAll(s);
        }

        pub fn writeType(self: *Self, value: anytype) !void {
            const T = @TypeOf(value);
            switch (@typeInfo(T)) {
                .Struct => |i| {
                    // todo: provide hook for custom struct writing
                    inline for (i.fields) |field| {
                        try self.writeType(@field(value, field.name));
                    }
                },
                .Int => |i| {
                    switch (i.bits) {
                        8 => try self.writeI8(value),
                        16 => try self.writeI16(value),
                        32 => try self.writeI32(value),
                        64 => try self.writeI64(value),
                        else => @compileError("int with these bits not supported"),
                    }
                },
                .Float => |f| {
                    switch (f.bits) {
                        64 => try self.writeF64(value),
                        else => @compileError("float with these bits not supported"),
                    }
                },
                .Bool => {
                    try self.writeBool(value);
                },
                .Pointer => |p| {
                    // fixme: how do we distinguish between string and bytes?
                    if (p.child == u8) {
                        try self.writeStr(value);
                    } else {
                        try self.writeI32(@intCast(value.len));
                        for (value) |el| {
                            try self.writeType(el);
                        }
                    }
                },
                else => |otherwise| {
                    std.debug.print("failed to write field of type {any}", .{otherwise});
                    @compileError("unsupported type " ++ @typeName(T));
                },
            }
        }
    };
}

test "type round trip" {
    const allocator = std.testing.allocator;
    var buffer = std.ArrayList(u8).init(allocator);

    var writer = Writer(std.ArrayList(u8).Writer).init(buffer.writer());
    const C = struct {
        name: []const u8 = "child",
    };
    const T = struct {
        b: bool = false,
        i8: i8 = 8,
        i16: i16 = 16,
        i32: i32 = 32,
        i64: i64 = 64,
        f64: f64 = 64.64,
        string: []const u8 = "string",
        bytes: []const u8 = "bytes",
        children: []const C = &([_]C{}),
    };
    try writer.writeType(
        T{},
    );

    var bytes = try buffer.toOwnedSlice();
    defer allocator.free(bytes);

    if (bytes.len < 1) {
        return;
    }

    var reader = Reader.init(allocator, bytes);
    try std.testing.expectEqualDeep(T{}, try reader.readType(T));
}

test "round trip" {
    const allocator = std.testing.allocator;
    var buffer = std.ArrayList(u8).init(allocator);

    var writer = Writer(std.ArrayList(u8).Writer).init(buffer.writer());

    try writer.writeBool(true);
    try writer.writeBool(false);
    try writer.writeBytes("rand");
    try writer.writeStr("str");
    try writer.writeI32(32);
    try writer.writeI64(64);
    try writer.writeI8(8);
    try writer.writeF64(5.123);

    var bytes = try buffer.toOwnedSlice();
    defer allocator.free(bytes);

    var reader = Reader.init(allocator, bytes);

    try std.testing.expectEqual(true, try reader.readBool());
    try std.testing.expectEqual(false, try reader.readBool());
    try std.testing.expectEqualSlices(u8, "rand", try reader.readBytes());
    try std.testing.expectEqualSlices(u8, "str", try reader.readStr());
    try std.testing.expectEqual(try reader.readI32(), 32);
    try std.testing.expectEqual(try reader.readI64(), 64);
    try std.testing.expectEqual(try reader.readI8(), 8);
    try std.testing.expectEqual(try reader.readF64(), 5.123);
    try std.testing.expect(reader.isEmpty());
}
