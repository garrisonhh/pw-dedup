const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    // build
    const exe_release = b.addExecutable(.{
        .name = "pw-dedup",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = .ReleaseFast,
    });

    const exe_debug = b.addExecutable(.{
        .name = "pw-dedup",
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = .Debug,
    });

    b.installArtifact(exe_release);
    const build_release = b.addInstallArtifact(exe_release);
    const build_debug = b.addInstallArtifact(exe_debug);

    const release_step = b.step("release", "build release version");
    const debug_step = b.step("debug", "build debug version");
    release_step.dependOn(&build_release.step);
    debug_step.dependOn(&build_debug.step);

    // run
    const run_cmd = b.addRunArtifact(exe_debug);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "run in debug mode");
    run_step.dependOn(&run_cmd.step);
}
