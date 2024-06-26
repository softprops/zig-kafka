/// https://kafka.apache.org/protocol#protocol_types
const std = @import("std");
const record = @import("record.zig");

/// A type to help distinguish between a string represented by `[]const u8` in zig and arbitrary
/// bytes also represented by `[]const u8` in zig
pub const Bytes = struct {
    bytes: []const u8,
};

/// Represents a value and a deferred release of memory the caller owns.
/// The caller owns calling `deinit()` after using the value.
pub fn Owned(comptime T: type) type {
    return struct {
        value: T,
        allocator: std.heap.ArenaAllocator,

        /// Values are assumed to have been derived from a Reader.
        /// Owned values outlive the reader and take ownership of the readers
        /// allocations and as such becomes reponsible for deinit'ing them
        pub fn fromReader(value: T, reader: Reader) @This() {
            return .{
                .value = value,
                .allocator = reader.allocator,
            };
        }

        /// Call this to free underlying memory after using the value
        /// for the purposes needed by the caller
        pub fn deinit(self: @This()) void {
            self.allocator.deinit();
        }
    };
}

/// Decodes kafka protocol types from bytes read from the wire
pub const Reader = struct {
    data: []const u8,
    allocator: std.heap.ArenaAllocator,
    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, data: []const u8) Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer allocator.free(data);
        // dup'ing data to guarantee we own this memory allowing us to outlive the
        // data's source
        const owned = arena.allocator().dupe(u8, data) catch unreachable;
        return .{ .allocator = arena, .data = owned };
    }

    /// Call this to free memory which may have been allocated during readType options
    /// Alternatively create a Owned type passing this reponsibility off to a caller
    pub fn deinit(self: *Self) void {
        self.allocator.deinit();
    }

    pub fn read(self: *Self, n: usize) ![]const u8 {
        if (n > self.data.len) {
            return error.EOF;
        } else {
            const slice = self.data[0..n];
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
        return std.mem.readInt(i16, (try self.read(2))[0..2], .big);
    }

    pub fn readI32(self: *Self) !i32 {
        return std.mem.readInt(i32, (try self.read(4))[0..4], .big);
    }

    pub fn readI64(self: *Self) !i64 {
        return std.mem.readInt(i64, (try self.read(8))[0..8], .big);
    }

    pub fn readF64(self: *Self) !f64 {
        return @bitCast(@constCast((try self.read(8))[0..8]).*);
    }

    // unsigned "zigzag" encoded int
    pub fn readUnsignedVarInt(self: *Self) !u32 {
        var val: u32 = 0;
        for (0..5) |i| {
            const b: u32 = @intCast((try self.read(1))[0]);
            val |= (b & 0x7F) << @intCast(i * 7);
            if (b < 0x80) {
                break;
            }
        }
        return val;
    }

    pub fn readVarInt(self: *Self) !i32 {
        const unsigned = try self.readUnsignedVarInt();
        return @as(i32, @intCast(unsigned >> 1)) ^ (-@as(i32, @intCast((unsigned & 1))));
    }

    pub fn readUnsignedVarLong(self: *Self) !u64 {
        var val: u64 = 0;
        for (0..10) |i| {
            const b: u64 = @intCast((try self.read(1))[0]);
            val |= (b & 0x7F) << @intCast(i * 7);
            if (b < 0x80) {
                break;
            }
        }
        return val;
    }

    pub fn readVarLong(self: *Self) !i64 {
        const unsigned = try self.readUnsignedVarLong();
        return @as(i64, @intCast(unsigned >> 1)) ^ (-@as(i64, @intCast((unsigned & 1))));
    }

    pub fn readNullableStr(self: *Self) !?[]const u8 {
        const len = try self.readI16();
        if (len == -1) {
            return null;
        }
        return if (len > 0) try self.read(@intCast(len)) else "";
    }

    pub fn readStr(self: *Self) ![]const u8 {
        const len = try self.readI16();
        return if (len > 0) self.read(@intCast(len)) else "";
    }

    pub fn readCompactStr(self: *Self) ![]const u8 {
        const len = try self.readUnsignedVarInt();
        return if (len > 0) self.read(@intCast(len - 1)) else "";
    }

    pub fn readBytes(self: *Self) ![]const u8 {
        const len = try self.readI32();
        return if (len > 0) self.read(@intCast(len)) else self.data[0..0];
    }

    pub fn readCompactBytes(self: *Self) ![]const u8 {
        const len = try self.readUnsignedVarInt();
        return if (len > 0) self.read(@intCast(len - 1)) else self.data[0..0];
    }

    pub fn readArrayLen(self: *Self) !i32 {
        return @max(try self.readI32(), 0);
    }

    pub fn isEmpty(self: *Self) bool {
        return self.data.len < 1;
    }

    /// this method allocates memory with reading slices
    pub fn readType(self: *Self, comptime T: type) !T {
        const info = @typeInfo(T);
        return switch (info) {
            .Optional => |o| {
                // we only support optional strs
                if (o.child == []const u8) {
                    return self.readNullableStr();
                }
                @compileError("optional types of " ++ @typeName(o.child) ++ " are not supported");
            },
            .Struct => |s| {
                if (T == Bytes) {
                    return Bytes{ .bytes = try self.readBytes() };
                }
                if (T == record.RecordBatch) {
                    // in versions >= 12 this is compact bytes
                    // otherwise this is regular bytes
                    std.debug.print("resolving batch remaining bytes {any}\n", .{self.data});
                    const recordBytes = try self.readBytes();
                    std.debug.print("recordBytes {any}\n", .{recordBytes});
                    var nextReader = Reader{
                        .data = recordBytes,
                        .allocator = self.allocator,
                    };
                    return try record.RecordBatch.init(&nextReader);
                }
                var parsed: T = undefined;
                // todo: provide hook for custom struct reading
                inline for (s.fields) |field| {
                    //std.debug.print("resolving field {s}\n", .{field.name});
                    const value = self.readType(field.type) catch |err| {
                        std.debug.print(
                            "failed to resolve value of type {s} for field {s}\n",
                            .{
                                field.name,
                                @typeName(field.type),
                            },
                        );
                        return err;
                    };
                    //std.debug.print("assigning field {s} to value {any}\n", .{ field.name, value });
                    @field(parsed, field.name) = value;
                }
                return parsed;
            },
            .Int => |i| switch (i.bits) {
                8 => self.readI8(),
                16 => self.readI16(),
                32 => self.readI32(),
                64 => self.readI64(),
                else => @compileError("int with these bits not supported"),
            },
            .Float => |f| switch (f.bits) {
                64 => self.readF64(),
                else => @compileError("float with these bits not supported"),
            },
            .Bool => self.readBool(),
            .Pointer => |p| {
                if (p.child == u8) {
                    return self.readStr();
                }
                //std.debug.print("reading slice len\n", .{});
                const len = try self.readI32();
                //std.debug.print("read slice len {d}\n", .{len});

                if (len < 1) {
                    // don't bother allocating if there's nothing to allocate
                    return &[_]p.child{};
                }

                var buf = try std.ArrayList(p.child).initCapacity(self.allocator.allocator(), @intCast(len));
                errdefer buf.deinit();
                for (0..@intCast(len)) |_| {
                    buf.appendAssumeCapacity(try self.readType(p.child));
                }
                return try buf.toOwnedSlice();
            },
            else => |otherwise| {
                std.debug.print("unable to parse type {any}", .{otherwise});
                @compileError("supported type " ++ @typeName(T));
            },
        };
    }
};

pub fn packU32(u: u32) [4]u8 {
    var buf: [4]u8 = undefined;
    var bufWriter = std.io.fixedBufferStream(&buf);
    bufWriter.writer().writeInt(u32, u, .big) catch unreachable;
    return buf;
}

/// Encodes kafka protocol types to a target writer
pub fn Writer(comptime W: type) type {
    return struct {
        writer: W,
        const Self = @This();
        pub fn init(writer: W) Self {
            return .{ .writer = writer };
        }

        fn writeByte(self: *Self, b: u8) !void {
            try self.writeByte(b);
        }

        pub fn writeBool(self: *Self, b: bool) !void {
            try self.writeI8(if (b) 1 else 0);
        }

        pub fn writeI8(self: *Self, i: i8) !void {
            try self.writer.writeInt(i8, i, .big);
        }

        pub fn writeI16(self: *Self, i: i16) !void {
            try self.writer.writeInt(i16, i, .big);
        }

        pub fn writeI32(self: *Self, i: i32) !void {
            try self.writer.writeInt(i32, i, .big);
        }

        pub fn writeI64(self: *Self, i: i64) !void {
            try self.writer.writeInt(i64, i, .big);
        }

        pub fn writeF64(self: *Self, f: f64) !void {
            try self.writer.writeAll(&@as([8]u8, @bitCast(f)));
        }

        // unsigned "zigzag" encoded int
        pub fn writeUnsignedVarInt(self: *Self, i: u32) !void {
            var value: u32 = i;
            while (value >= 0x80) {
                try self.writer.writeInt(u8, @as(u8, @intCast(value)) | 0x80, .big);
                value >>= 7;
            }
            try self.writer.writeInt(u8, @as(u8, @intCast(value)), .big);
        }

        pub fn writeVarInt(self: *Self, i: i32) !void {
            const value = i;
            const unsigned: u32 = @intCast((value << 1) ^ (value >> 31));
            return self.writeUnsignedVarInt(unsigned);
        }

        pub fn writeUnsignedVarLong(self: *Self, i: u64) !void {
            var value: u64 = i;
            while (value >= 0x80) {
                try self.writer.writeInt(u8, @as(u8, @intCast(value)) | 0x80, .big);
                value >>= 7;
            }
            try self.writer.writeInt(u8, @as(u8, @intCast(value)), .big);
        }

        pub fn writeVarLong(self: *Self, i: i64) !void {
            const value = i;
            const unsigned: u64 = @intCast((value << 1) ^ (value >> 63));
            return self.writeUnsignedVarLong(unsigned);
        }

        pub fn writeStr(self: *Self, s: []const u8) !void {
            const len: i16 = @intCast(s.len);
            try self.writer.writeInt(i16, len, .big);
            try self.writer.writeAll(s);
        }

        pub fn writeCompactStr(self: *Self, s: []const u8) !void {
            if (s.len == 0) {
                return try self.writeUnsignedVarInt(0);
            }
            try self.writeUnsignedVarInt(@as(u32, @intCast(s.len)) + 1);
            try self.writer.writeAll(s);
        }

        pub fn writeBytes(self: *Self, b: []const u8) !void {
            const len: i32 = @intCast(b.len);
            try self.writer.writeInt(i32, len, .big);
            try self.writer.writeAll(b);
        }

        pub fn writeCompactBytes(self: *Self, b: []const u8) !void {
            if (b.len == 0) {
                return try self.writeUnsignedVarInt(0);
            }
            try self.writeUnsignedVarInt(@as(u32, @intCast(b.len)) + 1);
            try self.writer.writeAll(b);
        }

        pub fn writeType(self: *Self, value: anytype) !void {
            const T = @TypeOf(value);
            switch (@typeInfo(T)) {
                .Struct => |i| {
                    if (T == Bytes) {
                        return try self.writeBytes(value.bytes);
                    }
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
    errdefer buffer.deinit();

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
        bytes: Bytes = .{ .bytes = "bytes" },
        children: []const C = &([_]C{
            .{
                .name = "t",
            },
        }),
    };
    try writer.writeType(
        T{},
    );

    const bytes = try buffer.toOwnedSlice();
    errdefer allocator.free(bytes);

    if (bytes.len < 1) {
        return;
    }

    var reader = Reader.init(allocator, bytes);
    defer reader.deinit();
    try std.testing.expectEqualDeep(T{}, try reader.readType(T));
}

test "round trip" {
    const allocator = std.testing.allocator;
    var buffer = std.ArrayList(u8).init(allocator);
    errdefer buffer.deinit();
    var writer = Writer(std.ArrayList(u8).Writer).init(buffer.writer());

    try writer.writeBool(true);
    try writer.writeBool(false);
    try writer.writeBytes("bytes");
    try writer.writeCompactBytes("cbytes");
    try writer.writeStr("str");
    try writer.writeCompactStr("cstr");
    try writer.writeI32(32);
    try writer.writeI64(64);
    try writer.writeI8(8);
    try writer.writeF64(5.123);
    try writer.writeUnsignedVarInt(32);
    try writer.writeUnsignedVarLong(64);
    try writer.writeVarInt(-32);
    try writer.writeVarLong(-64);

    const bytes = try buffer.toOwnedSlice();
    errdefer allocator.free(bytes);

    var reader = Reader.init(allocator, bytes);
    defer reader.deinit();

    try std.testing.expectEqual(true, try reader.readBool());
    try std.testing.expectEqual(false, try reader.readBool());
    try std.testing.expectEqualSlices(u8, "bytes", try reader.readBytes());
    try std.testing.expectEqualSlices(u8, "cbytes", try reader.readCompactBytes());
    try std.testing.expectEqualSlices(u8, "str", try reader.readStr());
    try std.testing.expectEqualSlices(u8, "cstr", try reader.readCompactStr());
    try std.testing.expectEqual(try reader.readI32(), 32);
    try std.testing.expectEqual(try reader.readI64(), 64);
    try std.testing.expectEqual(try reader.readI8(), 8);
    try std.testing.expectEqual(try reader.readF64(), 5.123);
    try std.testing.expectEqual(try reader.readUnsignedVarInt(), 32);
    try std.testing.expectEqual(try reader.readUnsignedVarLong(), 64);
    try std.testing.expectEqual(try reader.readVarInt(), -32);
    try std.testing.expectEqual(try reader.readVarLong(), -64);
    try std.testing.expect(reader.isEmpty());
}
