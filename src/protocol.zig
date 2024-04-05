//! Implementation of the [Kafka protocol](https://kafka.apache.org/protocol)
const std = @import("std");

const apiversions = @import("apiversions.zig");
const metadata = @import("metadata.zig");

pub const ApiVersionsRequest = apiversions.Request;
pub const ApiVersionsResponse = apiversions.Response;

pub const MetadataRequest = metadata.Request;
pub const MetadataReponse = metadata.Response;

pub const MessageHeaders = struct {
    apiKey: i16,
    apiVersion: i16,
    correlationId: i32,
    clientId: []const u8,

    pub fn init(
        apiKey: ApiKey,
        apiVersion: i16,
        correlationId: i32,
        clientId: []const u8,
    ) @This() {
        return .{
            .apiKey = apiKey.toInt(),
            .apiVersion = apiVersion,
            .correlationId = correlationId,
            .clientId = clientId,
        };
    }
};

/// The following enumerates the codes that the ApiKey in the request can take for available request types.
///
/// see also https://kafka.apache.org/protocol#protocol_api_keys
pub const ApiKey = enum(i16) {
    //produce = 0,
    //fetch = 1,
    //offset = 2,
    metadata = 3,
    //leaderAndIsr = 4,
    //stopReplica = 5,
    //updateMetadata = 6,
    //controlledShutdown = 7,
    //offsetCommit = 8,
    //offsetFetch = 9,
    //findCoordinator = 10,
    //joinGroup = 11,
    //heartbeat = 12,
    //leaveGroup = 13,
    //syncGroup = 14,
    //describeGroups = 15,
    //listGroups = 16,
    //saslHandshake = 17,
    apiVersions = 18,
    //createTopics = 19,
    //deleteTopics = 20,
    //deleteRecords = 21,
    //initProducerId = 22,
    //offsetForLeaderEpoch = 23,
    //addPartitionsToTxn = 24,
    //addOffsetsToTxn = 25,
    //endTxn = 26,
    //writeTxnMarkers = 27,
    //txnOffsetCommit = 28,
    //describeAcls = 29,
    //createAcls = 30,
    //deleteAcls = 31,
    //describeConfigs = 32,
    //alterConfigs = 33,
    //alterReplicaLogDirs = 34,
    //describeLogDirs = 35,
    //saslAuthenticate = 36,
    //createPartitions = 37,
    //createDelegationToken = 38,
    //renewDelegationToken = 39,
    //expireDelegationToken = 40,
    //describeDelegationToken = 41,
    //deleteGroups = 42,
    //electLeaders = 43,
    //incrementalAlterConfigs = 44,
    //alterPartitionReassignments = 45,
    //listPartitionReassignments = 46,
    //offsetDelete = 47,

    /// the serialized format of these apikeys
    pub fn toInt(self: @This()) i16 {
        return @intFromEnum(self);
    }

    pub fn requestType(comptime self: @This()) type {
        return switch (self) {
            //produce => ???,
            //fetch => ???,
            //offset => ???,
            .metadata => MetadataRequest,
            //leaderAndIsr => ???,
            //stopReplica => ???,
            //updateMetadata => ???,
            //controlledShutdown => ???,
            //offsetCommit => ???,
            //offsetFetch => ???,
            //findCoordinator => ???,
            //joinGroup => ???,
            //heartbeat => ???,
            //leaveGroup => ???,
            //syncGroup => ???,
            //describeGroups => ???,
            //listGroups => ???,
            //saslHandshake => ???,
            .apiVersions => ApiVersionsRequest,
            //createTopics => ???,
            //deleteTopics => ???,
            //deleteRecords => ???,
            //initProducerId => ???,
            //offsetForLeaderEpoch => ???,
            //addPartitionsToTxn => ???,
            //addOffsetsToTxn => ???,
            //endTxn => ???,
            //writeTxnMarkers => ???,
            //txnOffsetCommit => ???,
            //describeAcls => ???,
            //createAcls => ???,
            //deleteAcls => ???,
            //describeConfigs => ???,
            //alterConfigs => ???,
            //alterReplicaLogDirs => ???,
            //describeLogDirs => ???,
            //saslAuthenticate => ???,
            //createPartitions => ???,
            //createDelegationToken => ???,
            //renewDelegationToken => ???,
            //expireDelegationToken => ???,
            //describeDelegationToken => ???,
            //deleteGroups => ???,
            //electLeaders => ???,
            //incrementalAlterConfigs => ???,
            //alterPartitionReassignments => ???,
            //listPartitionReassignments => ???,
            //offsetDelete => ???,
        };
    }

    pub fn responseType(comptime self: @This()) type {
        return switch (self) {
            //produce => ???,
            //fetch => ???,
            //offset => ???,
            .metadata => MetadataReponse,
            //leaderAndIsr => ???,
            //stopReplica => ???,
            //updateMetadata => ???,
            //controlledShutdown => ???,
            //offsetCommit => ???,
            //offsetFetch => ???,
            //findCoordinator => ???,
            //joinGroup => ???,
            //heartbeat => ???,
            //leaveGroup => ???,
            //syncGroup => ???,
            //describeGroups => ???,
            //listGroups => ???,
            //saslHandshake => ???,
            .apiVersions => ApiVersionsResponse,
            //createTopics => ???,
            //deleteTopics => ???,
            //deleteRecords => ???,
            //initProducerId => ???,
            //offsetForLeaderEpoch => ???,
            //addPartitionsToTxn => ???,
            //addOffsetsToTxn => ???,
            //endTxn => ???,
            //writeTxnMarkers => ???,
            //txnOffsetCommit => ???,
            //describeAcls => ???,
            //createAcls => ???,
            //deleteAcls => ???,
            //describeConfigs => ???,
            //alterConfigs => ???,
            //alterReplicaLogDirs => ???,
            //describeLogDirs => ???,
            //saslAuthenticate => ???,
            //createPartitions => ???,
            //createDelegationToken => ???,
            //renewDelegationToken => ???,
            //expireDelegationToken => ???,
            //describeDelegationToken => ???,
            //deleteGroups => ???,
            //electLeaders => ???,
            //incrementalAlterConfigs => ???,
            //alterPartitionReassignments => ???,
            //listPartitionReassignments => ???,
            //offsetDelete => ???,
        };
    }
};

/// An enumeration of error codes indicating what problem occurred on the server.
///
/// see also https://kafka.apache.org/protocol#protocol_error_codes
pub const ErrorCode = enum(i8) {
    unknown_server_error = -1,
    none = 0,
    offset_out_of_range = 1,
    corrupt_message = 2,
    unknown_topic_or_parition = 3,
    invalid_fetch_size = 4,
    leader_not_available = 5,

    pub fn fromCode(c: i8) ?ErrorCode {
        return @enumFromInt(c);
    }

    fn isRetryable(self: @This()) bool {
        return switch (self) {
            .corrupt_message, .unknown_topic_or_parition, .leader_not_available => true,
            else => false,
        };
    }

    pub fn isError(self: @This()) bool {
        return self != .none;
    }

    pub fn nonNone(c: i8) ?ErrorCode {
        if (ErrorCode.from(c)) |ec| {
            if (ec.isError()) {
                return ec;
            }
        }
        return null;
    }
};

test "error" {
    const tests = [_]struct {
        expect: ErrorCode,
        code: i8,
        retryable: bool = false,
        err: bool = true,
    }{
        .{
            .expect = .unknown_server_error,
            .code = -1,
        },
        .{
            .expect = .none,
            .code = 0,
            .err = false,
        },
    };
    for (tests) |t| {
        try std.testing.expectEqual(
            ErrorCode.fromCode(t.code),
            t.expect,
        );
        try std.testing.expectEqual(
            ErrorCode.fromCode(t.code).?.isRetryable(),
            t.retryable,
        );
        try std.testing.expectEqual(
            ErrorCode.fromCode(t.code).?.isError(),
            t.err,
        );
    }
}
