const std = @import("std");
const global_context = @import("./global_context.zig");
const BoundedQueue = @import("./lock_free.zig").BoundedQueue;

pub const MetricKind = enum(u8) {
    requestProcessingTime,
    requestCounter,
    taskProcessingTime,
    queueWaitTime,
    memtableCounter,

    pub fn asInt(comptime T: type, self: MetricKind) T {
        return @as(T, @intFromEnum(self));
    }
};

pub const MetricRecord = packed struct {
    timestamp: i64,
    value: u64,
    kind: u32,
};

const Histogram = struct {
    allocator: std.mem.Allocator,
    buckets: []u64,
    bucket_bounds: []u64,

    pub inline fn init(allocator: std.mem.Allocator, bounds: []const u64) !Histogram {
        const buckets = try allocator.alloc(u64, bounds.len + 1);
        const bucket_bounds = try allocator.alloc(u64, bounds.len);

        @memset(buckets, 0);
        @memcpy(bucket_bounds, bounds);

        return .{
            .allocator = allocator,
            .buckets = buckets,
            .bucket_bounds = bucket_bounds,
        };
    }

    pub inline fn deinit(self: *Histogram) void {
        self.allocator.free(self.buckets);
        self.allocator.free(self.bucket_bounds);
    }

    pub inline fn drop(self: *Histogram) void {
        @memset(self.buckets, 0);
    }

    pub fn record(self: *const Histogram, value: u64) void {
        for (0..self.bucket_bounds.len) |i| {
            if (value <= self.bucket_bounds[i]) {
                self.buckets[i] += 1;
                return;
            }
        }
        self.buckets[self.buckets.len - 1] += 1;
    }
};

// bucket sizes in microseconds
const DEFAULT_LATENCY_BOUNDS: [14]u64 = [_]u64{
    1,       5,       10,    50,    100,    500,
    1000,    5000,    10000, 50000, 100000, 500000,
    1000000, 5000000,
};

const Metrics = struct {
    allocator: std.mem.Allocator,

    timestamp: i64,
    request_latency: Histogram,
    task_latency: Histogram,
    queue_wait: Histogram,

    memtable_count: u64,
    request_count: u64,

    pub fn init(allocator: std.mem.Allocator) !Metrics {
        return .{
            .allocator = allocator,
            .timestamp = std.time.microTimestamp(),
            .request_latency = try Histogram.init(allocator, &DEFAULT_LATENCY_BOUNDS),
            .task_latency = try Histogram.init(allocator, &DEFAULT_LATENCY_BOUNDS),
            .queue_wait = try Histogram.init(allocator, &DEFAULT_LATENCY_BOUNDS),
            .memtable_count = 0,
            .request_count = 0,
        };
    }

    pub fn drop(self: *Metrics) void {
        self.timestamp = std.time.microTimestamp();
        self.request_latency.drop();
        self.task_latency.drop();
        self.queue_wait.drop();
        self.request_count = 0;
    }

    pub fn deinit(self: *Metrics) void {
        self.request_latency.deinit();
        self.task_latency.deinit();
        self.queue_wait.deinit();
    }
};

const Percentile = enum(u8) {
    p50,
    p95,
    p99,

    pub fn numeratorDenominator(self: Percentile) struct { num: u64, den: u64 } {
        return switch (self) {
            .p50 => .{ .num = 50, .den = 100 },
            .p95 => .{ .num = 50, .den = 100 },
            .p99 => .{ .num = 50, .den = 100 },
        };
    }
};
const NUM_PERCENTILES = @typeInfo(Percentile).@"enum".fields.len;

const MetricsSnapshot = struct {
    const Percentiles = [NUM_PERCENTILES]u64;

    timestamp: i64,
    request_latency_percentiles: Percentiles,
    task_latency_percentiles: Percentiles,
    queue_wait_percentiles: Percentiles,
    request_count: u64,
    memtable_count: u64,

    pub fn init(metrics: *Metrics) MetricsSnapshot {
        return .{
            .timestamp = metrics.timestamp,
            .request_latency_percentiles = populatePercentiles(&metrics.request_latency),
            .task_latency_percentiles = populatePercentiles(&metrics.task_latency),
            .queue_wait_percentiles = populatePercentiles(&metrics.queue_wait),
            .request_count = metrics.request_count,
            .memtable_count = metrics.memtable_count,
        };
    }

    fn populatePercentiles(histogram: *Histogram) Percentiles {
        var percentiles: Percentiles = [_]u64{0} ** NUM_PERCENTILES;

        var total: u64 = 0;
        for (histogram.buckets) |count| {
            total += count;
        }
        if (total == 0) return percentiles;

        var thresholds: Percentiles = [_]u64{0} ** NUM_PERCENTILES;
        for (0..thresholds.len) |i| {
            const frac = Percentile.numeratorDenominator(@enumFromInt(i));
            thresholds[i] = (total / frac.den) * frac.num;
        }

        var cumulative: u64 = 0;
        var j: usize = 0;
        for (histogram.buckets, 0..) |count, i| {
            cumulative += count;
            while (j < thresholds.len and cumulative >= thresholds[j]) : (j += 1) {
                percentiles[j] = histogram.bucket_bounds[i];
            }
            if (j >= thresholds.len) break;
        }

        while (j < thresholds.len) : (j += 1) {
            percentiles[j] = histogram.bucket_bounds[histogram.bucket_bounds.len - 1];
        }

        return percentiles;
    }
};

pub const MetricsAggregator = struct {
    allocator: std.mem.Allocator,
    channel: BoundedQueue(MetricRecord),
    metrics: Metrics,
    last_snapshot: i64,

    pub inline fn init(allocator: std.mem.Allocator, capacity: usize) !MetricsAggregator {
        return .{
            .allocator = allocator,
            .channel = try BoundedQueue(MetricRecord).init(allocator, capacity),
            .metrics = try Metrics.init(allocator),
            .last_snapshot = std.time.microTimestamp(),
        };
    }

    pub inline fn start(self: *MetricsAggregator) void {
        self.metrics.timestamp = std.time.microTimestamp();

        var metric: ?MetricRecord = undefined;
        while (true) {
            metric = self.channel.dequeue();
            if (metric) |m| {
                switch (m.kind) {
                    MetricKind.asInt(u32, .requestProcessingTime) => self.metrics.request_latency.record(m.value),
                    MetricKind.asInt(u32, .taskProcessingTime) => self.metrics.task_latency.record(m.value),
                    MetricKind.asInt(u32, .queueWaitTime) => self.metrics.queue_wait.record(m.value),
                    MetricKind.asInt(u32, .requestCounter) => self.metrics.request_count += 1,
                    MetricKind.asInt(u32, .memtableCounter) => self.metrics.memtable_count += 1,
                    else => {},
                }
            }

            if (self.tickPassed()) {
                const snapshot = MetricsSnapshot.init(&self.metrics);
                const message = std.json.stringifyAlloc(self.allocator, snapshot, .{}) catch unreachable;
                defer self.allocator.free(message);
                std.debug.print("{s}\n", .{message});
                self.metrics.drop();
            }
        }
    }

    pub inline fn stop(self: *MetricsAggregator) void {
        self.channel.deinit();
        self.metrics.deinit();
    }

    pub inline fn record(self: *MetricsAggregator, metric: MetricRecord) bool {
        return self.channel.enqueue(metric);
    }

    fn tickPassed(self: *MetricsAggregator) bool {
        const now = std.time.microTimestamp();
        if (now - self.last_snapshot >= std.time.us_per_s) {
            self.last_snapshot = now;
            return true;
        }
        return false;
    }
};

pub fn metricsTask() void {
    const metrics_aggregator = global_context.getMetricsAggregator().?;
    metrics_aggregator.start();
}

// Tests
const testing = std.testing;

test "Metrics" {
    var metrics = Metrics{
        .allocator = testing.allocator,
        .timestamp = std.time.microTimestamp(),
        .request_latency = try Histogram.init(testing.allocator, &DEFAULT_LATENCY_BOUNDS),
        .task_latency = try Histogram.init(testing.allocator, &DEFAULT_LATENCY_BOUNDS),
        .queue_wait = try Histogram.init(testing.allocator, &DEFAULT_LATENCY_BOUNDS),
        .memtable_count = 0,
        .request_count = 0,
    };
    defer metrics.deinit();

    const snapshot = MetricsSnapshot.init(&metrics);

    const message = try std.json.stringifyAlloc(testing.allocator, snapshot, .{});
    defer testing.allocator.free(message);

    std.debug.print("{s}\n", .{message});
}
