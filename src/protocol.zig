//! Implementation of the [Kafka protocol](https://kafka.apache.org/protocol)
const std = @import("std");

const apiversions = @import("apiversions.zig");
/// ApiVersions request type
pub const ApiVersionsRequest = apiversions.Request;
/// ApiVersions response type
pub const ApiVersionsResponse = apiversions.Response;

/// The following enumerates the codes that the ApiKey in the request can take for available request types.
///
/// see also https://kafka.apache.org/protocol#protocol_api_keys
pub const ApiKey = enum(i16) {
    produce = 0,
    fetch = 1,
    offset = 2,
    metadata = 3,
    leaderAndIsr = 4,
    stopReplica = 5,
    updateMetadata = 6,
    controlledShutdown = 7,
    offsetCommit = 8,
    offsetFetch = 9,
    findCoordinator = 10,
    joinGroup = 11,
    heartbeat = 12,
    leaveGroup = 13,
    syncGroup = 14,
    describeGroups = 15,
    listGroups = 16,
    saslHandshake = 17,
    apiVersions = 18,
    createTopics = 19,
    deleteTopics = 20,
    deleteRecords = 21,
    initProducerId = 22,
    offsetForLeaderEpoch = 23,
    addPartitionsToTxn = 24,
    addOffsetsToTxn = 25,
    endTxn = 26,
    writeTxnMarkers = 27,
    txnOffsetCommit = 28,
    describeAcls = 29,
    createAcls = 30,
    deleteAcls = 31,
    describeConfigs = 32,
    alterConfigs = 33,
    alterReplicaLogDirs = 34,
    describeLogDirs = 35,
    saslAuthenticate = 36,
    createPartitions = 37,
    createDelegationToken = 38,
    renewDelegationToken = 39,
    expireDelegationToken = 40,
    describeDelegationToken = 41,
    deleteGroups = 42,
    electLeaders = 43,
    incrementalAlterConfigs = 44,
    alterPartitionReassignments = 45,
    listPartitionReassignments = 46,
    offsetDelete = 47,

    /// the serialized format of these apikeys
    pub fn toInt(self: @This()) i16 {
        return @intFromEnum(self);
    }
};

/// An enumeration of error codes indicating what problem occurred on the server.
///
/// see also https://kafka.apache.org/protocol#protocol_error_codes
pub const Error = enum(i8) {
    unknown_server_error = -1,
    none = 0,
    offset_out_of_range = 1,
    corrupt_message = 2,

    fn retryable(self: @This()) bool {
        return switch (self) {
            .corrupt_message => true,
            else => false,
        };
    }
};

test "error" {
    const tests = [_]struct {
        expect: Error,
        code: i8,
        retryable: bool,
    }{
        .{
            .expect = .unknown_server_error,
            .code = -1,
            .retryable = false,
        },
    };
    for (tests) |t| {
        try std.testing.expectEqual(
            @as(Error, @enumFromInt(t.code)),
            t.expect,
        );
        try std.testing.expectEqual(
            @as(Error, @enumFromInt(t.code)).retryable(),
            t.retryable,
        );
    }
}
