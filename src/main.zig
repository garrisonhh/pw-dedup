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

fn extrude(path: []const u8) !void {
    var prog = std.Progress{};
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

    const Block = struct {
        /// required for munmap, not usable (contains garbage at the end)
        memory: []align(std.mem.page_size) u8,
        /// useful text
        text: []const u8,
    };

    const InitError = std.os.OpenError || std.os.MMapError || std.os.SeekError;
    /// if you get an OOM error, it is likely that this is too large
    const max_block_bytes = 1024 * std.mem.page_size;

    fd: std.os.fd_t,
    blocks: std.ArrayListUnmanaged(Block),

    /// open and mmap a file from a path
    /// TODO split this up for readability if I need to return to it
    fn init(ally: Allocator, path: []const u8) InitError!Self {
        // open file for reading
        const fd = try std.os.open(path, std.os.linux.O.RDONLY, 0);

        // get length
        try std.os.lseek_END(fd, 0);
        const byte_length = try std.os.lseek_CUR_get(fd);
        try std.os.lseek_SET(fd, 0);

        // mmap blocks
        const PROT = std.os.linux.PROT;
        const MAP = std.os.linux.MAP;
        
        var blocks = std.ArrayListUnmanaged(Block){};

        var offset: usize = 0; // offset of current block
        var text_offset: usize = 0; // offset of text into the next block
        while (offset < byte_length) {
            // acq memory
            const max_bytes = @min(byte_length - offset, max_block_bytes);
            const memory = try std.os.mmap(
                null,
                max_bytes,
                PROT.READ,
                MAP.SHARED,
                fd,
                offset,
            );
            
            std.debug.print("loaded block: {} -> ", .{offset});

            // trim to line boundary if there's another block after this one
            var text: []const u8 = memory[text_offset..];
            if (offset + memory.len < byte_length) {
                // get memory window for text
                var boundary = memory.len - 1;
                while (boundary > 0 and memory[boundary] != '\n') {
                    boundary -= 1;
                }

                text = memory[text_offset..boundary];

                // find next offset and text offset
                const aligned_offset =
                    @divFloor(boundary, std.mem.page_size) *
                    std.mem.page_size;

                offset += aligned_offset;
                text_offset = boundary - aligned_offset;
            } else {
                // we're done
                offset = byte_length;
            }

            std.debug.print("{}\n", .{offset});

            try blocks.append(ally, Block{
                .memory = memory,
                .text = text,
            });
        }

        return Self{
            .fd = fd,
            .blocks = blocks,
        };
    }

    /// munmap and close the file
    fn deinit(self: *Self, ally: Allocator) void {
        for (self.blocks.items) |block| std.os.munmap(block.memory);
        self.blocks.deinit(ally);
        std.os.close(self.fd);
    }

    /// iterate over passwords
    fn iterator(self: Self) Iterator {
        return .{ .blocks = self.blocks.items };
    }

    const Iterator = struct {
        /// blocks remaining
        blocks: []const Block,
        /// current tokenizer
        tokenizer: ?std.mem.TokenIterator(u8, .scalar) = null,

        fn next(iter: *Iterator) ?[]const u8 {
            // null case; get next block
            if (iter.tokenizer == null) {
                if (iter.blocks.len == 0) {
                    return null;
                }

                const block = iter.blocks[0];
                iter.blocks = iter.blocks[1..];

                iter.tokenizer = std.mem.tokenizeScalar(u8, block.text, '\n');
            }

            // get a token
            if (iter.tokenizer.?.next()) |token| {
                return token;
            }

            // no tokens remaining, recurse to try the next block
            iter.tokenizer = null;
            return iter.next();
        }
    };
};

var files: std.ArrayListUnmanaged(MappedFile) = .{};

fn loadFile(
    ally: Allocator,
    path: []const u8,
)(Allocator.Error || MappedFile.InitError)!void {
    const file = try MappedFile.init(ally, path);
    try files.append(ally, file);
}

fn unloadFiles(ally: Allocator) void {
    for (files.items) |*file| file.deinit(ally);
    files.deinit(ally);
}

fn dedupFile(
    bump_ally: Allocator, // bump allocator for chains
    prog: *Progress.Node,
    file_display_name: []const u8,
    file: MappedFile,
) !void {
    // progress
    var est_passwords: usize = 0;

    {
        var iter = file.iterator();
        while (iter.next()) |_| est_passwords += 1;
    }
    
    var buf: [512]u8 = undefined;
    const desc = try std.fmt.bufPrint(
        &buf,
        "processing {s}",
        .{file_display_name},
    );

    var prog_node = prog.start(desc, est_passwords);
    prog_node.activate();
    defer prog_node.end();
    
    // do work
    {
        var iter = file.iterator();
        while (iter.next()) |password| {
            try addPassword(bump_ally, password);
            prog_node.completeOne();
        }
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
    var dedup_prog = std.Progress{};
    const dedup_node = dedup_prog.start("deduplicating files", in_filepaths.len);
    defer dedup_node.end();

    defer unloadFiles(ally);

    for (in_filepaths) |in_filepath| {
        try loadFile(ally, in_filepath);
    }

    // do the stuff
    for (files.items, 0..) |file, i| {
        try dedupFile(arena_ally, dedup_node, in_filepaths[i], file);
    }

    // write to the output file
    try extrude(out_filepath);
}
