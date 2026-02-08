const std = @import("std");

const global_context = @import("./global_context.zig");
const io = @import("./io.zig");

const BoundedQueue = @import("./lock_free.zig").BoundedQueue;
const RingBuffer = @import("./lock_free.zig").RingBuffer;
const Task = @import("./task_queue.zig").Task;

pub const MetricKind = enum(u8) {
    requestProcessingTime,
    requestCounter,
    taskProcessingTime,
    queueWaitTime,
    memtableCounter,
    workerCounter,

    pub fn asInt(comptime T: type, self: MetricKind) T {
        return @as(T, @intFromEnum(self));
    }
};

pub const MetricRecord = packed struct {
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
const DEFAULT_LATENCY_BOUNDS: [22]u64 = [_]u64{
    10,     50,     100,     150,     200,  250,  300,  400,   500,
    1000,   1500,   2000,    2500,    3000, 4000, 5000, 10000, 50000,
    100000, 500000, 1000000, 5000000,
};

const Metrics = struct {
    allocator: std.mem.Allocator,

    timestamp: i64,
    request_latency: Histogram,
    task_latency: Histogram,
    queue_wait: Histogram,

    memtable_count: u64,
    request_count: u64,
    worker_count: u64,

    pub fn init(allocator: std.mem.Allocator) !Metrics {
        return .{
            .allocator = allocator,
            .timestamp = std.time.microTimestamp(),
            .request_latency = try Histogram.init(allocator, &DEFAULT_LATENCY_BOUNDS),
            .task_latency = try Histogram.init(allocator, &DEFAULT_LATENCY_BOUNDS),
            .queue_wait = try Histogram.init(allocator, &DEFAULT_LATENCY_BOUNDS),
            .memtable_count = 0,
            .request_count = 0,
            .worker_count = 0,
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

pub const Percentile = enum(u8) {
    p50,
    p95,
    p99,

    pub fn numeratorDenominator(self: Percentile) struct { num: u64, den: u64 } {
        return switch (self) {
            .p50 => .{ .num = 50, .den = 100 },
            .p95 => .{ .num = 95, .den = 100 },
            .p99 => .{ .num = 99, .den = 100 },
        };
    }
};
const NUM_PERCENTILES = @typeInfo(Percentile).@"enum".fields.len;

pub const MetricsSnapshot = struct {
    const Percentiles = [NUM_PERCENTILES]u64;

    timestamp: i64,
    request_latency_percentiles: Percentiles,
    task_latency_percentiles: Percentiles,
    queue_wait_percentiles: Percentiles,
    request_count: u64,
    memtable_count: u64,
    worker_count: u64,

    pub fn init(metrics: *Metrics) MetricsSnapshot {
        return .{
            .timestamp = metrics.timestamp,
            .request_latency_percentiles = populatePercentiles(&metrics.request_latency),
            .task_latency_percentiles = populatePercentiles(&metrics.task_latency),
            .queue_wait_percentiles = populatePercentiles(&metrics.queue_wait),
            .request_count = metrics.request_count,
            .memtable_count = metrics.memtable_count,
            .worker_count = metrics.worker_count,
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
                const bound_idx = @min(i, histogram.bucket_bounds.len - 1);
                percentiles[j] = histogram.bucket_bounds[bound_idx];
            }
            if (j >= thresholds.len) break;
        }

        while (j < thresholds.len) : (j += 1) {
            percentiles[j] = histogram.bucket_bounds[histogram.bucket_bounds.len - 1];
        }

        return percentiles;
    }
};

pub const SnapshotBuffer = struct {
    allocator: std.mem.Allocator,
    buffer: RingBuffer(MetricsSnapshot),

    pub fn init(allocator: std.mem.Allocator, buffer_capacity: usize) !SnapshotBuffer {
        return .{
            .allocator = allocator,
            .buffer = try RingBuffer(MetricsSnapshot).init(allocator, buffer_capacity),
        };
    }

    pub inline fn deinit(self: *SnapshotBuffer) void {
        self.buffer.deinit();
    }

    pub inline fn capacity(self: *SnapshotBuffer) usize {
        return self.buffer.mask + 1;
    }

    pub fn push(self: *SnapshotBuffer, value: *const MetricsSnapshot) void {
        self.buffer.push(value);
    }

    pub fn readUntil(self: *const SnapshotBuffer, timestamp: i64, out: []MetricsSnapshot) usize {
        const head = @atomicLoad(usize, &self.buffer.head, .acquire);
        if (head == 0 or out.len == 0) return 0;

        var offset: usize = 1;
        var written: usize = 0;

        while (true) {
            const snapshot = self.buffer.readLatestOffset(offset);
            if (snapshot) |s| {
                out[written] = s;
                written += 1;

                if (s.timestamp <= timestamp) break;
                if (written == out.len) break;
            } else {
                break;
            }

            offset += 1;
        }

        return written;
    }
};

pub const MetricsAggregator = struct {
    allocator: std.mem.Allocator,
    channel: BoundedQueue(MetricRecord),
    metrics: Metrics,
    last_snapshot: i64,
    snapshot_buffer: SnapshotBuffer,

    pub inline fn init(allocator: std.mem.Allocator, capacity: usize, snapshot_capacity: usize) !MetricsAggregator {
        return .{
            .allocator = allocator,
            .channel = try BoundedQueue(MetricRecord).init(allocator, capacity),
            .metrics = try Metrics.init(allocator),
            .last_snapshot = std.time.microTimestamp(),
            .snapshot_buffer = try SnapshotBuffer.init(allocator, snapshot_capacity),
        };
    }

    pub inline fn start(self: *MetricsAggregator) void {
        self.metrics.timestamp = std.time.microTimestamp();

        var metric: ?MetricRecord = undefined;
        var idle_iterations: u64 = 0;
        while (true) {
            metric = self.channel.dequeue();
            if (metric) |m| {
                idle_iterations = 0;
                switch (m.kind) {
                    MetricKind.asInt(u32, .requestProcessingTime) => self.metrics.request_latency.record(m.value),
                    MetricKind.asInt(u32, .taskProcessingTime) => self.metrics.task_latency.record(m.value),
                    MetricKind.asInt(u32, .queueWaitTime) => self.metrics.queue_wait.record(m.value),
                    MetricKind.asInt(u32, .requestCounter) => self.metrics.request_count += 1,
                    MetricKind.asInt(u32, .memtableCounter) => {
                        if (m.value == 1) {
                            self.metrics.memtable_count += 1;
                        } else if (m.value == 0 and self.metrics.memtable_count > 0) {
                            self.metrics.memtable_count -= 1;
                        }
                    },
                    MetricKind.asInt(u32, .workerCounter) => {
                        if (m.value == 1) {
                            self.metrics.worker_count += 1;
                        } else if (m.value == 0 and self.metrics.worker_count > 0) {
                            self.metrics.worker_count -= 1;
                        }
                    },
                    else => {},
                }
            } else {
                idle_iterations += 1;
                if (idle_iterations < 20) {
                    std.atomic.spinLoopHint();
                    continue;
                } else if (idle_iterations < 100) {
                    std.Thread.yield() catch {};
                    continue;
                }
                std.Thread.sleep(50 * std.time.ns_per_us);
            }

            if (self.tickPassed()) {
                const snapshot = MetricsSnapshot.init(&self.metrics);
                self.snapshot_buffer.push(&snapshot);
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

pub inline fn recordMetric(aggregator: ?*MetricsAggregator, kind: MetricKind, value: u64) void {
    if (aggregator) |a| {
        _ = a.record(.{
            .value = value,
            .kind = @intFromEnum(kind),
        });
    }
}

pub const MetricRequestError = error{
    MetricRequestFailed,
};

pub const MetricRequestTask = struct {
    allocator: std.mem.Allocator,
    io_context: *io.IO.IoContext,
    timestamp: i64,

    pub fn init(allocator: std.mem.Allocator, timestamp: i64, io_context: *io.IO.IoContext) !MetricRequestTask {
        return .{
            .allocator = allocator,
            .io_context = io_context,
            .timestamp = timestamp,
        };
    }

    pub fn task(self: *MetricRequestTask) Task {
        return .{
            .context = self,
            .run_fn = run,
            .destroy_fn = destroy,
            .enqued_at = std.time.microTimestamp(),
        };
    }

    fn run(ptr: *anyopaque) void {
        const self: *MetricRequestTask = @ptrCast(@alignCast(ptr));
        const buffer: []MetricsSnapshot = self.allocator.alloc(
            MetricsSnapshot,
            global_context.getMetricsAggregator().?.snapshot_buffer.capacity() / 2,
        ) catch |e| {
            std.log.err("Error! Failed to allocate metrics snapshot buffer: {any}", .{e});
            self.io_context.enqueueResponse(
                i8,
                MetricRequestError,
                -1,
                MetricRequestError.MetricRequestFailed,
            );
            return;
        };
        defer self.allocator.free(buffer);

        const num_snapshpts = global_context.getMetricsAggregator().?.snapshot_buffer.readUntil(self.timestamp, buffer);
        self.io_context.enqueueResponse(
            []MetricsSnapshot,
            MetricRequestError,
            buffer[0..num_snapshpts],
            null,
        );
    }

    fn destroy(ptr: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *MetricRequestTask = @ptrCast(@alignCast(ptr));
        allocator.destroy(self);
    }
};

pub fn runMetricAggregator() void {
    const metrics_aggregator = global_context.getMetricsAggregator().?;
    metrics_aggregator.start();
}
