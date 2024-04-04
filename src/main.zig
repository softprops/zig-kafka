//! An apache Kafka client for zig
//!
//! ```zig
//! const std = @import("std");
//! const kafka = @import("kafka");
//!
//! pub fn main() void {
//!     // ...
//! }
//! ```
const std = @import("std");
pub const protocol = @import("protocol.zig");
pub const codec = @import("codec.zig");
pub const client = @import("client.zig");

test {
    std.testing.refAllDecls(@This());
}
