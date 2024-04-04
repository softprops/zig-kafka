//! Metadata api
//!
//! see also https://kafka.apache.org/protocol#The_Messages_Metadata

pub const Request = struct { // v8
    /// The topics to fetch metadata for.
    topic_names: []const []const u8 = &([_][]const u8{}),
    /// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    allow_auto_topic_creation: bool = false,
    /// Whether to include cluster authorized operations.
    include_cluster_authorized_operations: bool = false,
    /// Whether to include topic authorized operations.
    include_topic_authorized_operations: bool = false,
};

pub const Response = struct { // vll
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    throttle_time_ms: i32,
    /// Each broker in the response.
    brokers: []const ResponseBroker,
    /// The cluster ID that responding broker belongs to.
    cluster_id: ?[]const u8 = null,
    /// The ID of the controller broker.
    controller_id: i32,
    /// Each topic in the response.
    topics: []const ResponseTopic,
    cluster_authorized_operations: i32,
};

pub const ResponseBroker = struct {
    /// The broker ID.
    node_id: i32,
    /// The broker hostname.
    host: []const u8,
    /// The broker port.
    port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    rack: ?[]const u8 = null,
};

pub const ResponseTopic = struct {
    /// The topic error, or 0 if there was no error.
    error_code: i16,
    /// The topic name.
    name: ?[]const u8, // compact nullable str
    /// The topic id.
    topic_id: []const u8, // uuid
    /// True if the topic is internal.
    is_internal: bool,
    /// Each partition in the topic.
    partitions: []const ResponsePartition,
    /// 32-bit bitfield to represent authorized operations for this topic.
    topic_authorized_operations: i32,
};

pub const ResponsePartition = struct {
    /// The partition error, or 0 if there was no error.
    error_code: i16,
    /// The partition index.
    partition_index: i32,
    /// The ID of the leader broker.
    leader_id: i32,
    /// The leader epoch of this partition.
    leader_epoch: i32,
    /// The set of all nodes that host this partition.
    replica_nodes: []const i32,
    /// The set of nodes that are in sync with the leader for this partition.
    isr_nodes: []const i32,
    /// The set of offline replicas of this partition.
    offline_replicates: []const i32,
};
