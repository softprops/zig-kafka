const std = @import("std");

/// https://kafka.apache.org/protocol#The_Messages_Fetch
/// assume version 16, api key = 1
pub const FetchRequest = struct {
    //header: HeaderRequest,
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    replica_id: i32 = -1,
    /// The maximum time in milliseconds to wait for the response.
    max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    min_bytes: i32,
    /// The topics to fetch where keys are The name of the topic to fetch and values are the partitions to fetch.
    topics: std.StringHashMap(TopicPartitionFetchRequest),
};

pub const TopicPartitionFetchRequest = struct {
    /// The partition index where the keys are the partition inde and values are contain the partiion info
    partitions: std.AutoHashMap(i32, PartitionFetchRequest),
};

pub const PartitionFetchRequest = struct {
    /// The message offset.
    fetch_offset: i32,
    /// The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
    partition_max_bytes: i32 = std.math.maxInt(i32),
};
