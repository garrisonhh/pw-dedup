const std = @import("std");
const stdout = std.io.getStdOut().writer();
const stderr = std.io.getStdErr().writer();
const Allocator = std.mem.Allocator;
const Progress = std.Progress;

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
        list: std.ArrayListUnmanaged(Password) = .{},
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

        for (chain.list.items) |pw| {
            if (pw.hash.eql(hash) and std.mem.eql(u8, pw.str(), str)) {
                // found match!
                break;
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
            for (chain.list.items) |pw| {
                try strm.print("{s}\n", .{ pw.str() });
                prog_node.completeOne();
            }
        }

        try buf_writer.flush();
    }
};

// file management =============================================================

const MappedFile = struct {
    const Self = @This();

    const Block = struct {
        /// required for munmap, not usable (contains garbage at the end)
        memory: []align(std.mem.page_size) u8,
        /// useful text
        text: []const u8,
    };

    const InitError =
        std.os.OpenError ||
        std.os.MMapError ||
        std.os.SeekError ||
        error { LongLineInInput };

    /// if you get an OOM error, it is likely that this is too large.
    ///
    /// however, this *must* be more than one page_size for iteration to work
    /// when creating blocks
    const max_block_bytes = 4096 * std.mem.page_size;

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

            // trim to line boundary if there's another block after this one
            var text: []const u8 = memory[text_offset..];
            if (offset + memory.len < byte_length) {
                // get memory window for text (ends at final newline) 
                var boundary = memory.len - 1;

                while (boundary > text_offset and memory[boundary] != '\n') {
                    boundary -= 1;
                }

                text = memory[text_offset..boundary];

                // find next offset and text offset
                const aligned_offset =
                    @divFloor(boundary, std.mem.page_size) *
                    std.mem.page_size;

                if (aligned_offset == 0) {
                    return InitError.LongLineInInput;
                }

                offset += aligned_offset;
                text_offset = boundary - aligned_offset;
            } else {
                // we're done
                offset = byte_length;
            }

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

/// it fucks those files lmao
const FileFucker = struct {
    const Self = @This();

    files: std.ArrayListUnmanaged(MappedFile) = .{},

    fn deinit(self: *Self, ally: Allocator) void {
        for (self.files.items) |*file| file.deinit(ally);
        self.files.deinit(ally);
    }

    /// queue a file to be deduped
    fn add(self: *Self, ally: Allocator, path: []const u8) !void {
        const file = try MappedFile.init(ally, path);
        try self.files.append(ally, file);
    }

    fn dedupBlock(set: *PasswordSet, block: MappedFile.Block) !void {
        var passwords = std.mem.tokenizeScalar(u8, block.text, '\n');
        while (passwords.next()) |str| {
            if (str.len == 0) continue;
            try set.add(str);
        }
    }

    fn dedupEntry(set: *PasswordSet, b: *Blockerator) !void {
        while (b.next()) |block| {
            try dedupBlock(set, block);
        }
        
        try std.Thread.yield();
    }

    /// threaded dedup with all the files
    fn dedup(self: *const Self, set: *PasswordSet) !void {
        var progress = std.Progress{};
        var b = self.blockerator(&progress);

        const cpu_count = try std.Thread.getCpuCount();    
        var threads = std.BoundedArray(std.Thread, 256){};

        for (0..cpu_count) |_| {
            const thread = try std.Thread.spawn(.{}, dedupEntry, .{set, &b});
            try threads.append(thread);
        }

        for (threads.slice()) |thread| {
            thread.join();
        }
    }

    fn blockerator(self: *const Self, progress: *Progress) Blockerator {
        var b = Blockerator{
            .files = self.files.items,
            .prog_node = undefined,
        };

        // count blocks
        var total_blocks: usize = 0;
        for (b.files) |file| {
            total_blocks += file.blocks.items.len;
        }

        // start progress
        b.prog_node = progress.start("processing blocks", total_blocks);

        return b;
    }

    /// thread-safe block queue
    const Blockerator = struct {
        mutex: std.Thread.Mutex = .{},
        
        prog_node: *Progress.Node,
        
        files: []const MappedFile,
        file: usize = 0,
        block: usize = 0,
        done: bool = false,

        fn next(b: *Blockerator) ?MappedFile.Block {
            b.mutex.lock();
            defer b.mutex.unlock();

            if (b.done) return null;
            
            // get block
            const blocks = b.files[b.file].blocks.items;
            const block = blocks[b.block];

            b.prog_node.completeOne();

            // iterate
            b.block += 1;
            if (b.block >= blocks.len) {
                b.file += 1;
                b.block = 0;

                if (b.file >= b.files.len) {
                    b.done = true;
                    b.prog_node.end();
                }
            }

            return block;
        }
    };
};

// progress ====================================================================

pub fn main() !void {
    const ally = gpa.allocator();
    defer _ = gpa.deinit();

    var arena = std.heap.ArenaAllocator.init(ally);
    defer arena.deinit();
    const arena_ally = arena.allocator();

    var set = try PasswordSet.init(arena_ally);
    defer set.deinit();

    var fucker = FileFucker{};
    defer fucker.deinit(ally);

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

    // load files
    for (in_filepaths) |in_filepath| {
        try fucker.add(ally, in_filepath);
    }

    // do the stuff
    try fucker.dedup(&set);

    // write to the output file
    try set.extrude(out_filepath);
}
