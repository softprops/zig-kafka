const std = @import("std");
const record = @import("record.zig");

// https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java#L130

pub const IsolationLevel = enum(i8) {
    read_uncommited = 0,
    read_commited = 1,

    fn toInt(self: @This()) i8 {
        return @intFromEnum(self);
    }
};

/// https://kafka.apache.org/protocol#The_Messages_Fetch
// see the following for inspriration around defaults https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java#L135-L140
pub const Request = struct { // v11
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    replica_id: i32 = -1,
    /// The maximum time in milliseconds to wait for the response.
    max_wait_ms: i32 = 1_000,
    /// The minimum bytes to accumulate in the response.
    min_bytes: i32 = 0,
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    max_bytes: i32 = std.math.maxInt(i32),
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
    isolation_level: i8 = IsolationLevel.read_uncommited.toInt(),
    /// The fetch session ID.1
    session_id: i32 = 0, // https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/clients/src/main/java/org/apache/kafka/common/requests/FetchMetadata.java#L29
    /// The fetch session epoch, which is used for ordering requests in a session.
    session_epoch: i32 = -1, // https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/clients/src/main/java/org/apache/kafka/common/requests/FetchMetadata.java#L43
    /// The topics to fetch where keys are The name of the topic to fetch and values are the partitions to fetch.
    topics: []const RequestTopic = &([_]RequestTopic{}),
    /// In an incremental fetch request, the partitions to remove.
    forgotten_topics: []const RequestForgottenTopic = &([_]RequestForgottenTopic{}),
    /// Rack ID of the consumer making this request
    rack_id: []const u8 = "",
};

pub const RequestTopic = struct {
    /// The name of the topic to fetch.
    topic: []const u8,
    /// The partitions to fetch. (you can retrieve this information from metadata requests for a given topic)
    partitions: []const RequestPartition = &([_]RequestPartition{}),
};

pub const RequestPartition = struct { // v11
    /// The partition index.
    partition: i32,
    /// The current leader epoch of the partition.
    current_leader_epoch: i32 = -1,
    /// The message offset.
    fetch_offset: i64,
    /// The earliest available offset of the follower replica. The field is only used when the request is sent by the follower.
    log_start_offset: i64,
    /// The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
    partition_max_bytes: i32 = std.math.maxInt(i32),
};

pub const RequestForgottenTopic = struct {
    /// The topic name.
    topic: []const u8,
    /// The partitions indexes to forget.
    paritions: []const i32,
};

pub const Response = struct { // v11
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    throttle_time_ms: i32,
    /// The top level response error code.
    error_code: i16,
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    session_id: i32,
    /// The response topics.
    responses: []const ResponseTopic,
};

pub const ResponseTopic = struct {
    /// The topic name.
    topic: []const u8,
    /// The topic partitions.
    partitions: []const ResponsePartition,
};

pub const ResponsePartition = struct {
    /// The partition index.
    partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    error_code: i16,
    /// The current high water mark.
    high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)
    last_stable_offset: i64,
    /// The current log start offset.
    log_start_offset: i64,
    /// The aborted transactions.
    aborted_transations: []const ResponseTransaction,
    /// The preferred read replica for the consumer to use on its next fetch request
    preferred_read_replica: i32,
    /// The record data.
    record_set: record.RecordBatch,
};

pub const ResponseTransaction = struct {
    /// The producer id associated with the aborted transaction.
    producer_id: i64,
    /// The first offset in the aborted transaction.
    first_offset: i64,
};
