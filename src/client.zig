const std = @import("std");
const codec = @import("codec.zig");
const protocol = @import("protocol.zig");

pub const Client = struct {
    const Self = @This();

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
    var bootstrap = [_][]const u8{"localhost:9092"};
    const addrs = try parseAddrs(allocator, &bootstrap);
    defer allocator.free(addrs);
    for (addrs) |addr| {
        const stream = try std.net.tcpConnectToAddress(addr);
        defer stream.close();

        // send(apiKey, apiVersion, req: anytype, respType: type)

        // send request

        const correlationId: i32 = 1;
        const request = protocol.ApiVersionsRequest{};
        const apiVersion: i16 = 2;
        const apiKey: protocol.ApiKey = .apiVersions;
        const responseType = protocol.ApiVersionsResponse;
        const clientId = "clientId";

        var reqBuf = std.ArrayList(u8).init(allocator);
        defer reqBuf.deinit();
        var writer = codec.Writer(std.ArrayList(u8).Writer).init(reqBuf.writer());

        // message size
        try writer.writeI32(0); // place holder for request size, we'll fill this out after writing out the rest of the request bytes

        // headers
        try writer.writeI16(apiKey.toInt());
        try writer.writeI16(apiVersion);
        try writer.writeI32(correlationId);
        try writer.writeStr(clientId);

        // message
        try writer.writeType(request);

        var reqBytes = try reqBuf.toOwnedSlice();
        defer allocator.free(reqBytes);

        // rewrite the first 4 bytes to be the encoded request len
        const sizeBytes = codec.packU32(@as(u32, @intCast(reqBytes.len)) - 4);
        for (sizeBytes, 0..) |b, i| {
            reqBytes[i] = b;
        }
        try stream.writer().writeAll(reqBytes);
        std.debug.print("wrote req {any}\n", .{reqBytes});

        // process response

        var streamReader = stream.reader();

        // read response size
        var respSize = try streamReader.readIntBig(i32);

        var respBuf = try std.ArrayList(u8).initCapacity(allocator, @intCast(respSize));
        defer respBuf.deinit();
        try respBuf.resize(@intCast(respSize));
        var respBytes = try respBuf.toOwnedSlice();
        defer allocator.free(respBytes);

        _ = try streamReader.read(respBytes);
        std.debug.print("read bytes {any}\n", .{respBytes});

        var reader = codec.Reader.init(allocator, respBytes);
        //defer reader.deinit();

        // headers
        try std.testing.expect(try reader.readI32() == correlationId);

        // response
        const response = try reader.readType(responseType);
        const owned = codec.Owned(responseType).init(response, reader.allocator);
        defer owned.deinit();
        for (owned.value.api_keys) |apikey| {
            std.debug.print("apikey {any}\n", .{apikey});
        }
    }
}
