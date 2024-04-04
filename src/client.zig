//! Main entrypoint for interacting with Kafka servers
const std = @import("std");
const codec = @import("codec.zig");
const protocol = @import("protocol.zig");

pub const Client = struct {
    const Self = @This();

    /// Configuration options available to clients
    pub const Options = struct {
        /// list of bootstrap cluster urls to get broker information from
        bootstrap_urls: [][]const u8,
    };

    const Connection = struct {
        host: []const u8,
        stream: std.net.Stream,
    };

    allocator: std.mem.Allocator,
    options: Options,
    addrs: []std.net.Address,

    fn init(
        allocator: std.mem.Allocator,
        options: Options,
    ) !Client {
        return .{
            .allocator = allocator,
            .options = options,
            .addrs = try parseAddrs(allocator, options.bootstrap_urls),
        };
    }

    fn deinit(self: *Self) void {
        self.allocator.free(self.addrs);
    }
};

// caller should free result
fn parseAddrs(
    allocator: std.mem.Allocator,
    bootstrap: [][]const u8,
) ![]std.net.Address {
    var all = std.ArrayList(std.net.Address).init(allocator);
    defer all.deinit();
    for (bootstrap) |b| {
        // name:port
        var components = std.mem.split(u8, b, ":");
        const addrs = try std.net.getAddressList(
            allocator,
            components.next().?,
            try std.fmt.parseInt(u16, components.next() orelse "9092", 10),
        );
        defer addrs.deinit();
        // zig resolves each name to two addres ipv{4,6}. we pick just one
        try all.appendSlice(addrs.addrs[0..1]);
    }
    return try all.toOwnedSlice();
}

// https://ziglang.cc/zig-cookbook/04-02-tcp-client.html

test "bootstrap" {
    if (std.os.getenv("CI")) |_| {
        return error.SkipZigTest;
    }
    const allocator = std.testing.allocator;
    var bootstrap = [_][]const u8{
        "localhost:9092",
    };
    const addrs = try parseAddrs(allocator, &bootstrap);
    defer allocator.free(addrs);

    for (addrs) |addr| {
        const stream = try std.net.tcpConnectToAddress(addr);
        defer stream.close();

        // roundtrip
        const request = protocol.MetadataRequest{
            // .topic_names = &([_][]const u8{"test"}),
        };
        const responseType = protocol.MetadataReponse;
        const headers = protocol.MessageHeaders.init(.metadata, 8, 1, "clientId");

        var reqBuf = std.ArrayList(u8).init(allocator);
        defer reqBuf.deinit();
        var writer = codec.Writer(std.ArrayList(u8).Writer).init(reqBuf.writer());

        // message (placeholder) size
        try writer.writeI32(0);
        try writer.writeType(headers);
        try writer.writeType(request);

        const reqBytes = try reqBuf.toOwnedSlice();
        defer allocator.free(reqBytes);

        // rewrite size message message segment with the actual message size
        const sizeBytes = codec.packU32(@as(u32, @intCast(reqBytes.len)) - 4);
        for (sizeBytes, 0..) |b, i| {
            reqBytes[i] = b;
        }

        try stream.writer().writeAll(reqBytes);

        std.debug.print("...\n", .{});
        std.debug.print("sent {any}\n", .{reqBytes});

        // response size
        const respSize = try stream.reader().readIntBig(i32);

        var respBuf = try std.ArrayList(u8).initCapacity(allocator, @intCast(respSize));
        defer respBuf.deinit();
        try respBuf.resize(@intCast(respSize));
        var respBytes = try respBuf.toOwnedSlice();
        defer allocator.free(respBytes);

        _ = try stream.reader().read(respBytes);
        std.debug.print("recv {any}\n", .{respBytes});

        var reader = codec.Reader.init(allocator, respBytes);

        // headers
        try std.testing.expect(try reader.readI32() == headers.correlationId);

        // response
        const response = try reader.readType(responseType);
        const owned = codec.Owned(responseType).init(response, reader.allocator);
        defer owned.deinit();
        std.debug.print("response {}\n", .{owned.value});
        for (owned.value.brokers) |broker| {
            std.debug.print("broker {s}:{d}\n", .{ broker.host, broker.port });
        }
    }
}
