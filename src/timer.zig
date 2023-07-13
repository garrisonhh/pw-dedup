const std = @import("std");

/// returns time in seconds
pub fn now() f64 {
    const ts = @floatFromInt(f64, std.time.nanoTimestamp());
    return ts * 1e-9;
}