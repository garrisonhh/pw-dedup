const std = @import("std");
const stdout = std.io.getStdOut().writer();
const stderr = std.io.getStdErr().writer();
const Allocator = std.mem.Allocator;
const Progress = std.Progress;

// map mechanism ===============================================================

const hash_seed = 0xBEEF_FA7; // lol
const chain_count = 512 * 1024;

const Hash = struct {
    const Self = @This();

    n: u64,

    fn init(str: []const u8) Self {
        const n = std.hash.Wyhash.hash(hash_seed, str);
        return .{ .n = n };
    }

    fn eql(self: Self, other: Self) bool {
        return self.n == other.n;
    }
};

const Node = struct {
    const Self = @This();

    next: ?*Node,
    hash: Hash,
    password: []const u8,
};

var password_count: usize = 0;
var chains: [chain_count]?*Node = .{null} ** chain_count;

fn addPassword(
    ally: Allocator,
    password: []const u8,
) !void {
    // password validity check
    if (password.len == 0) return;

    // find or create a node
    const hash = Hash.init(password);
    const index = hash.n % chain_count;

    var trav = chains[index];
    while (trav) |cnode| : (trav = cnode.next) {
        if (cnode.hash.eql(hash) and
            std.mem.eql(u8, cnode.password, password))
        {
            // node found
            break;
        }
    } else {
        // must create node
        const cnode = try ally.create(Node);
        cnode.* = .{
            .next = chains[index],
            .hash = hash,
            .password = password, // password string owned by os
        };

        chains[index] = cnode;
        password_count += 1;
    }
}

fn extrude(prog: *Progress.Node, path: []const u8) !void {
    var prog_node = prog.start("extruding", password_count);
    defer prog_node.end();

    // buffered write to output
    const out_file = try std.fs.cwd().createFile(path, .{});
    defer out_file.close();

    var buf_writer = std.io.bufferedWriter(out_file.writer());
    const strm = buf_writer.writer();

    // iterate over counter and write
    for (chains) |list| {
        var trav = list;
        while (trav) |node| : (trav = node.next) {
            try strm.print("{s}\n", .{ node.password });

            prog_node.completeOne();
        }
    }

    try buf_writer.flush();
}

// file management =============================================================

const MappedFile = struct {
    const Self = @This();

    const InitError = std.os.OpenError || std.os.MMapError || std.os.SeekError;

    fd: std.os.fd_t,
    text: []align(std.mem.page_size) const u8,

    /// open and mmap a file from a path
    fn init(path: []const u8) InitError!Self {
        // open file for reading
        const fd = try std.os.open(path, std.os.linux.O.RDONLY, 0);

        // get length
        try std.os.lseek_END(fd, 0);
        const byte_length = try std.os.lseek_CUR_get(fd);
        try std.os.lseek_SET(fd, 0);

        // mmap
        const text = try std.os.mmap(
            null,
            byte_length,
            std.os.linux.PROT.READ,
            std.os.linux.MAP.SHARED,
            fd,
            0,
        );

        return Self{
            .fd = fd,
            .text = text,
        };
    }

    /// munmap and close the file
    fn deinit(self: Self) void {
        std.os.munmap(self.text);
        std.os.close(self.fd);
    }
};

var files: std.ArrayListUnmanaged(MappedFile) = .{};

fn loadFile(
    ally: Allocator,
    path: []const u8,
)(Allocator.Error || MappedFile.InitError)!void {
    const file = try MappedFile.init(path);
    try files.append(ally, file);
}

fn unloadFiles(ally: Allocator) void {
    for (files.items) |file| file.deinit();
    files.deinit(ally);
}

fn dedupFile(
    bump_ally: Allocator, // bump allocator for chains
    prog: *Progress.Node,
    file: MappedFile,
) !void {
    // progress
    const est_passwords = std.mem.count(u8, file.text, "\n");

    var node = prog.start("deduping password occurrences", est_passwords);
    defer node.end();

    // do work
    var tokenizer = std.mem.tokenizeScalar(u8, file.text, '\n');
    while (tokenizer.next()) |pw| {
        try addPassword(bump_ally, pw);
        node.completeOne();
    }
}

// progress ====================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const ally = gpa.allocator();
    defer _ = gpa.deinit();

    var arena = std.heap.ArenaAllocator.init(ally);
    defer arena.deinit();
    const arena_ally = arena.allocator();

    var prog = std.Progress{};
    const prog_root = prog.start("password deduplication", 0);
    defer prog_root.end();

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

    // file management
    defer unloadFiles(ally);
    for (in_filepaths) |in_filepath| {
        try loadFile(ally, in_filepath);
    }

    // do the stuff
    for (files.items, 0..) |file, i| {
        var buf: [512]u8 = undefined;
        const desc = try std.fmt.bufPrint(
            &buf,
            "processing {s}",
            .{in_filepaths[i]},
        );

        var prog_node = prog_root.start(desc, 0);
        defer prog_node.end();

        try dedupFile(arena_ally, &prog_node, file);
    }

    try extrude(prog_root, out_filepath);
}
