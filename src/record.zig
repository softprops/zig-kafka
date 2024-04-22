const codec = @import("codec.zig");
const std = @import("std");

pub const Compression = enum {
    none,
    snappy,
    gzip,
    zstd,
    lz4,
};

pub const TimestampType = enum {
    append,
    creation,
};

// Attributes is a bitset representing special attributes set on records.
pub const Attributes = struct {
    compression: Compression,
    timestampType: TimestampType,
    transactional: bool,
    control: bool,

    fn init(attrs: i16) @This() {
        return .{
            .compression = switch (attrs & 0x7) {
                0 => .none,
                1 => .gzip,
                2 => .snappy,
                3 => .lz4,
                4 => .zstd,
                else => unreachable,
            },
            .timestampType = if (((attrs & (1 << 3))) != 0)
                .append
            else
                .creation,
            .transactional = (attrs & (1 << 4)) != 0,
            .control = (attrs & (1 << 5)) != 0,
        };
    }
};

// todo: expose additional properties
pub const Record = struct {
    offset: i64,
    timestamp: i64,
    key: []const u8,
    value: []const u8,
    headers: []const Header = &([_]Header{}),
};

pub const Header = struct {
    key: []const u8, // string
    value: []const u8, // bytes
};

// magicByteOffset is the position of the magic byte in all versions of record
// sets in the kafka protocol.
const MAGIC_BYTE_OFFSET: usize = 16;

/// Exposes an aggregation of collected Records
pub const RecordBatch = struct {
    records: []const Record,
    pub fn init(reader: *codec.Reader) !@This() {
        var batchReader = RecordBatchReader{ .reader = reader };
        const records = try batchReader.read();
        return .{ .records = records };
    }
};

pub const RecordBatchReader = struct {
    reader: *codec.Reader,

    /// The lifetime of returned records will be attached to reader's allocator
    pub fn read(self: *@This()) ![]const Record {
        var records = std.ArrayList(Record).init(self.reader.allocator.allocator());
        errdefer records.deinit();
        while (!self.reader.isEmpty()) {
            try self.readBatch(&records);
        }
        return try records.toOwnedSlice();
    }

    fn readBatch(self: *@This(), records: *std.ArrayList(Record)) !void {
        std.debug.print("remaining bytes {any}\n", .{self.reader.data});
        if (self.reader.data.len < MAGIC_BYTE_OFFSET) {
            std.debug.print("too few bytes for batch {any}\n", .{self.reader.data});
            // discard remaining
            _ = try self.reader.read(self.reader.data.len);
            return;
        }
        const version: i8 = @intCast(self.reader.data[MAGIC_BYTE_OFFSET]);
        std.debug.print("version of batch is {d}\n", .{version});
        switch (version) {
            0, 1 => {
                try self.readLegacyBatch(version, records);
            },
            2 => {
                try self.readNewBatch(version, records);
            },
            else => {
                std.debug.print("unknown record batch version {d}", .{version});
                return error.InvalidBatchVersion;
            },
        }
    }

    // https://kafka.apache.org/documentation/#messageset
    fn readLegacyBatch(self: @This(), version: i8, records: *std.ArrayList(Record)) !void {
        const baseOffset = try self.reader.readI64();
        const batchLen = try self.reader.readI32();
        if (batchLen < 0) {
            return error.InvalidBatchSize;
        }
        var batchReader = codec.Reader{
            .data = try self.reader.read(@intCast(batchLen)),
            .allocator = self.reader.allocator,
        };

        // crc (todo: verify this)
        _ = try batchReader.readI32();

        const magic = try batchReader.readI8();
        if (magic != version) {
            return error.InvalidRecordVersion;
        }
        const attributes = Attributes.init(try batchReader.readI16());
        std.debug.print("todo: apply decompresson of type '{any}'\n", .{attributes.compression});
        const timestamp = if (version > 0) try batchReader.readI64() else -1; // no timestamp
        const key = try batchReader.readBytes();
        const value = try batchReader.readBytes();
        try records.append(.{
            .offset = baseOffset,
            .timestamp = timestamp,
            .key = key,
            .value = value,
        });
    }

    // https://kafka.apache.org/documentation/#recordbatch
    fn readNewBatch(self: *@This(), version: i8, records: *std.ArrayList(Record)) !void {
        const baseOffset = try self.reader.readI64();
        const batchLen = try self.reader.readI32();
        if (batchLen < 0) {
            return error.InvalidBatchSize;
        }
        var batchReader = codec.Reader{
            .data = try self.reader.read(@intCast(batchLen)),
            .allocator = self.reader.allocator,
        };

        const partition_leader_epoch = try batchReader.readI32();
        _ = partition_leader_epoch;
        const magic = try batchReader.readI8();
        if (magic != version) {
            return error.InvalidRecordVersion;
        }

        // crc (todo: verify this)
        _ = try batchReader.readI32();

        const attributes = Attributes.init(try batchReader.readI16());

        const lastOffsetDelta = try batchReader.readI32();
        _ = lastOffsetDelta;
        const baseTimestamp = try batchReader.readI64();
        const maxTimestamp = try batchReader.readI64();
        _ = maxTimestamp;
        const producerId = try batchReader.readI64();
        _ = producerId;
        const producerEpoch = try batchReader.readI16();
        _ = producerEpoch;
        const baseSequence = try batchReader.readI32();

        const recordCount = try batchReader.readI32();
        if (recordCount < 0) {
            return error.InvalidRecordCount;
        }
        std.debug.print("todo: apply decompresson of type '{any}'\n", .{attributes.compression});

        for (0..@intCast(recordCount)) |_| {
            const size = try batchReader.readVarInt();
            if (size < 0) {
                return error.InvalidRecordSize;
            }
            var recordReader = codec.Reader{
                .data = try batchReader.read(@intCast(size)),
                .allocator = batchReader.allocator,
            };

            // record attributes
            _ = try recordReader.readI8();

            const timestampDelta = try recordReader.readVarInt();
            const timestamp: i64 = baseTimestamp - @as(i64, timestampDelta);

            const offsetDelta = try recordReader.readVarInt();
            const offset = baseOffset + @as(i64, offsetDelta);
            const sequence = baseSequence +% offsetDelta;
            _ = sequence;

            const keyLen = try recordReader.readVarInt();
            const key = try recordReader.read(@intCast(keyLen));
            const valLen = try recordReader.readVarInt();
            const value = try recordReader.read(@intCast(valLen));

            const headerCount = try recordReader.readVarInt();
            var headers = std.ArrayList(Header).init(recordReader.allocator.allocator());
            for (0..@intCast(headerCount)) |_| {
                const headerKeyLen = try recordReader.readVarInt();
                const headerKey = try recordReader.read(@intCast(headerKeyLen));
                const headerValLen = try recordReader.readVarInt();
                const headerVal = try recordReader.read(@intCast(headerValLen));
                try headers.append(Header{
                    .key = headerKey,
                    .value = headerVal,
                });
                std.debug.print("header {s} => {any}", .{ headerKey, headerVal });
            }
            try records.append(.{
                .offset = offset,
                .timestamp = timestamp,
                .key = key,
                .value = value,
                .headers = try headers.toOwnedSlice(),
            });
        }
    }
};
