const std = @import("std");
const stdout = std.io.getStdOut().writer();
const stderr = std.io.getStdErr().writer();
const Allocator = std.mem.Allocator;
const Progress = std.Progress;
const BlockStream = @import("blockstream.zig");

// gpa =========================================================================

var gpa = std.heap.GeneralPurposeAllocator(.{
    .stack_trace_frames = 8,
}){};
/// required during threaded allocations
var gpa_lock: std.Thread.Mutex = .{};

// map mechanism ===============================================================

const Hash = packed struct(u32) {
    const Self = @This();

    n: u32,

    fn init(str: []const u8) Self {
        const n = std.hash.Crc32.hash(str);
        return .{ .n = n };
    }

    fn eql(self: Self, other: Self) bool {
        return self.n == other.n;
    }
};

const Password = struct {
    const Self = @This();

    const LengthInt = u32;

    hash: Hash,
    len: LengthInt,
    ptr: [*]const u8,

    fn str(self: Self) []const u8 {
        return self.ptr[0..self.len];
    }
};

const PasswordSet = struct {
    const Self = @This();
    const start_size = 1024 * 1024;

    const Chain = struct {
        mutex: std.Thread.Mutex = .{},
        list: std.MultiArrayList(Password) = .{},
    };

    ally: Allocator,
    chains: []Chain,
    count: usize = 0,

    fn init(ally: Allocator) Allocator.Error!Self {
        var self = Self{
            .ally = ally,
            .chains = try ally.alloc(Chain, start_size),
        };

        @memset(self.chains, .{});

        return self;
    }

    fn deinit(self: *Self) void {
        for (self.chains) |*chain| {
            chain.list.deinit(self.ally);
        }
        self.ally.free(self.chains);
    }

    fn add(self: *Self, str: []const u8) Allocator.Error!void {
        const hash = Hash.init(str);
        const chain = &self.chains[hash.n % self.chains.len];

        // find or append
        chain.mutex.lock();
        defer chain.mutex.unlock();

        for (chain.list.items(.hash), 0..) |other_hash, i| {
            if (other_hash.eql(hash)) {
                const other_str = chain.list.get(i).str();
                if (std.mem.eql(u8, str, other_str)) {
                    // found match!
                    break;
                }
            }
        } else {
            // no match, add a link
            self.count += 1;

            gpa_lock.lock();
            defer gpa_lock.unlock();

            try chain.list.append(self.ally, Password{
                .hash = hash,
                .ptr = str.ptr,
                .len = @intCast(Password.LengthInt, str.len),
            });
        }
    }

    fn extrude(self: Self, path: []const u8) !void {
        var prog = std.Progress{};
        var prog_node = prog.start("extruding", self.count);
        defer prog_node.end();

        // buffered write to output
        const out_file = try std.fs.cwd().createFile(path, .{});
        defer out_file.close();

        var buf_writer = std.io.bufferedWriter(out_file.writer());
        const strm = buf_writer.writer();

        // iterate over counter and write
        for (self.chains) |chain| {
            for (chain.list.items(.ptr), chain.list.items(.len)) |ptr, len| {
                const str = ptr[0..len];
                try strm.print("{s}\n", .{str});
                prog_node.completeOne();
            }
        }

        try buf_writer.flush();
    }
};

// file management =============================================================

fn dedupEntry(set: *PasswordSet, iter: *BlockStream.Iterator) !void {
    while (try iter.next()) |block| {
        var passwords = std.mem.tokenizeScalar(u8, block.text, '\n');
        while (passwords.next()) |password| {
            if (password.len == 0) continue;
            try set.add(password);
        }
    }
}

/// threaded dedup with all the files
fn dedup(set: *PasswordSet, paths: []const []const u8) !void {
    const size_hint = 512 * std.mem.page_size; // TODO make this configurable?
    var iter = BlockStream.iterator(paths, size_hint);

    const cpu_count = try std.Thread.getCpuCount();
    var threads = std.BoundedArray(std.Thread, 256){};

    for (0..cpu_count) |_| {
        const thread = try std.Thread.spawn(.{}, dedupEntry, .{ set, &iter });
        try threads.append(thread);
    }

    for (threads.slice()) |thread| {
        thread.join();
    }
}

// progress ====================================================================

pub fn main() !void {
    const ally = gpa.allocator();
    defer _ = gpa.deinit();

    var arena = std.heap.ArenaAllocator.init(ally);
    defer arena.deinit();
    const arena_ally = arena.allocator();

    var set = try PasswordSet.init(arena_ally);
    defer set.deinit();

    // get input file path from args
    const args = try std.process.argsAlloc(ally);
    defer std.process.argsFree(ally, args);

    if (args.len < 3) {
        try stderr.print(
            \\usage: {[exe_name]s} [out.csv] [a.txt] [b.txt] [...] [z.txt]
            \\    provide an output filepath, and this will dedup the passwords
            \\    on each line of each input file.
            \\
        ,
            .{
                .exe_name = args[0],
            },
        );

        std.process.exit(1);
        unreachable;
    }

    const out_filepath = args[1];
    const in_filepaths = args[2..];

    // do the stuff
    try dedup(&set, in_filepaths);

    // write to the output file
    try set.extrude(out_filepath);
}
