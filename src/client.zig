//! Main entrypoint for interacting with Kafka servers
const std = @import("std");
const codec = @import("codec.zig");
const protocol = @import("protocol.zig");
const fetch = @import("fetch.zig");

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
    errdefer all.deinit();
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
fn roundTrip(
    allocator: std.mem.Allocator,
    comptime apiKey: protocol.ApiKey,
    apiVersion: i16,
    correlationId: i32,
    clientId: []const u8,
    request: apiKey.requestType(),
    streamReader: anytype,
    streamWriter: anytype,
) !codec.Owned(apiKey.responseType()) {
    const Response = apiKey.responseType();
    const headers = protocol.MessageHeaders.init(
        apiKey,
        apiVersion,
        correlationId,
        clientId,
    );
    var reqBuf = std.ArrayList(u8).init(allocator);
    errdefer reqBuf.deinit();
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

    try streamWriter.writeAll(reqBytes);

    std.log.debug("sent {any}\n", .{reqBytes});

    const respLen = try streamReader.readInt(i32, .big);
    var respBuf = try std.ArrayList(u8).initCapacity(allocator, @intCast(respLen));
    errdefer respBuf.deinit();
    try respBuf.resize(respBuf.capacity);
    const respBytes = try respBuf.toOwnedSlice();

    _ = try streamReader.read(respBytes);
    std.log.debug("recv {any}\n", .{respBytes});

    var reader = codec.Reader.init(allocator, respBytes);

    // validate response is for this request
    if (try reader.readI32() != headers.correlationId) {
        return error.InvalidCorrelationId;
    }

    // response
    return codec.Owned(Response).fromReader(
        try reader.readType(Response),
        reader,
    );
}

test "roundTrip" {
    if (std.posix.getenv("CI")) |_| {
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

        //const timeout = std.os.timeval{ .tv_sec = @as(i32, 1), .tv_usec = @as(i32, 0) };
        //std.os.setsockopt(stream.handle, std.os.SOL.SOCKET, std.os.SO.RCVTIMEO, std.mem.asBytes(&timeout)) catch {};
        //std.os.setsockopt(stream.handle, std.os.SOL.SOCKET, std.os.SO.SNDTIMEO, std.mem.asBytes(&timeout)) catch {};

        const response: codec.Owned(protocol.FetchResponse) = try roundTrip(
            allocator,
            .fetch,
            // .metadata,
            11,
            //8,
            1,
            "clientId",
            protocol.FetchRequest{
                //.replica_id = 0,
                .topics = &([_]fetch.RequestTopic{
                    .{
                        .topic = "test",
                        // get these partition refs from metadata response
                        // https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java#L258
                        .partitions = &([_]fetch.RequestPartition{
                            // .{
                            //     .partition = 0,
                            //     .fetch_offset = 0,
                            //     .log_start_offset = 0,
                            // },
                            // .{
                            //     .partition = 1,
                            //     .fetch_offset = 0,
                            //     .log_start_offset = 0,
                            // },
                            .{
                                .partition = 2,
                                .fetch_offset = 0,
                                .log_start_offset = 0,
                            },
                        }),
                    },
                }),
            },
            // protocol.MetadataRequest{
            //     .topic_names = &([_][]const u8{"test"}),
            // },
            stream.reader(),
            stream.writer(),
        );
        defer response.deinit();
        std.debug.print("response {any}\n", .{response.value});
        for (response.value.responses) |topic| {
            for (topic.partitions) |partition| {
                std.debug.print("partition {any}\n", .{partition});
                for (partition.record_set.records) |record| {
                    std.debug.print("record {s} value {s}\n", .{ record.key, record.value });
                }
            }
        }

        // std.debug.print("topics\n", .{});
        // for (response.value.topics) |topic| {
        //     for (topic.partitions) |partition| {
        //         std.debug.print(" * {?s} on parition {any}\n", .{ topic.name, partition.partition_index });
        //     }
        // }
    }
}
