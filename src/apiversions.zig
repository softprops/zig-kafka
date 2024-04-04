pub const Request = struct {};

pub const ApiKeyResponse = struct {
    api_key: i16,
    min_version: i16,
    max_version: i16,
};

pub const Response = struct {
    error_code: i16,
    api_keys: []const ApiKeyResponse,
    throttle_time_ms: i32,
};
