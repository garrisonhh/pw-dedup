//! an iterator for streaming read-mmapped blocks of lines from a file

const std = @import("std");
const os = std.os;
const linux = os.linux;
const Allocator = std.mem.Allocator;
const page_size = std.mem.page_size;

const Self = @This();

const RawBlock = []align(page_size) const u8;

path: []const u8,
handle: os.fd_t,
length: usize,

const OpenError = os.OpenError || os.SeekError;

/// open file for streaming
fn open(path: []const u8) OpenError!Self {
    const fd = try os.open(path, linux.O.RDONLY, 0);

    // detect length
    try os.lseek_END(fd, 0);
    const length = try os.lseek_CUR_get(fd);
    try os.lseek_SET(fd, 0);

    return Self{
        .path = path,
        .handle = fd,
        .length = length,
    };
}

/// close stream
fn close(self: Self) void {
    os.close(self.handle);
}

/// small wrapper for easy mmap usage consistency
fn map(self: Self, offset: usize, length: usize) os.MMapError!RawBlock {
    return std.os.mmap(
        null,
        length,
        linux.PROT.READ,
        linux.MAP.SHARED,
        self.handle,
        offset,
    );
}

/// higher level block abstraction, better for consumption
pub const Block = struct {
    mem: []align(page_size) const u8,
    text: []const u8,

    fn map(bs: Self, range: BlockRange) os.MMapError!Block {
        const offset_diff = range.offset % page_size;
        const aligned_offset = range.offset - offset_diff;

        const mem = try bs.map(aligned_offset, offset_diff + range.length);
        const text = mem[offset_diff..offset_diff + range.length];

        return .{
            .mem = mem,
            .text = text,
        };
    }

    pub fn unmap(blk: Block) void {
        // TODO pretty sure this is thread-safe but not entirely
        os.munmap(blk.mem);
    }
};

const BlockRange = struct {
    offset: usize,
    length: usize,
};

const ScanError =
    Allocator.Error ||
    os.MMapError ||
    error{BadSizeHintAlignment};

/// find blocks appropriate for streaming. it attempts to allocate blocks at a
/// maximum size below size_hint, but if there is a line that is larger than
/// size_hint, will still create a range, at the smallest size possible.
///
/// size_hint must be a multiple of page_size for mmap alignment
/// returns owned slice from allocator
fn scan(
    self: Self,
    ally: Allocator,
    size_hint: usize,
) ScanError![]const BlockRange {
    if (size_hint % page_size != 0) {
        return ScanError.BadSizeHintAlignment;
    }

    var ranges = std.ArrayListUnmanaged(BlockRange){};

    var range_start: usize = 0;
    var range_len: usize = 0;
    var offset: usize = 0;
    while (offset < self.length) : (offset += size_hint) {
        const is_eof = offset + size_hint >= self.length;

        // map scannable block
        const mem_length = if (is_eof) self.length - offset else size_hint;
        const mem = try self.map(offset, mem_length);
        defer os.munmap(mem);

        // scan for next range
        for (mem, 0..) |ch, index| {
            if (ch == '\n') {
                const length = offset + index - range_start;
                if (length < size_hint) {
                    // range could still be larger
                    range_len = length;
                    continue;
                }

                // detected a range
                if (range_len == 0) {
                    range_len = length;
                }

                try ranges.append(ally, .{
                    .offset = range_start,
                    .length = range_len,
                });

                range_start += range_len;
                range_len = 0;
            }
        }

        // for the final range, just generate a range (if it exists)
        if (is_eof and range_start < self.length) {
            try ranges.append(ally, .{
                .offset = range_start,
                .length = self.length - range_start,
            });
        }
    }

    return try ranges.toOwnedSlice(ally);
}

// iterator ====================================================================

/// returns a thread-safe iterator over multiple files
/// paths should outlive iterator
pub fn iterator(
    /// list of files to read
    paths: []const []const u8,
    /// block size hint
    size_hint: usize,
) Iterator {
    return Iterator.init(paths, size_hint);
}

pub const Iterator = struct {
    pub const Error = OpenError || ScanError;

    paths_remaining: []const []const u8,
    size_hint: usize,

    /// state
    gpa: std.heap.GeneralPurposeAllocator(.{}) = .{},
    gpa_killed: bool = false,
    mutex: std.Thread.Mutex = .{},

    bs: ?Self = null,
    ranges: []const BlockRange = &.{},
    index: usize = 0,

    fn init(paths: []const []const u8, size_hint: usize) Iterator {
        return .{
            .paths_remaining = paths,
            .size_hint = size_hint,
        };
    }

    /// assumes mutex lock
    fn nextRange(iter: *Iterator) Error!?BlockRange {
        // there are ranges remaining, nothing to do but iterate
        if (iter.index < iter.ranges.len) {
            std.debug.print(
                "range {}/{} ({} files remain)\n",
                .{
                    iter.index,
                    iter.ranges.len,
                    iter.paths_remaining.len,
                },
            );

            defer iter.index += 1;
            return iter.ranges[iter.index];
        }

        // close/free any resources this is done with
        if (iter.bs) |bs| {
            bs.close();
            iter.bs = null;

            const ally = iter.gpa.allocator();
            ally.free(iter.ranges);
        }

        // check if streams remain
        if (iter.paths_remaining.len == 0) {
            if (!iter.gpa_killed) {
                _ = iter.gpa.deinit();        
                iter.gpa_killed = true;
            }
            return null;
        }

        // open new stream
        const ally = iter.gpa.allocator();
        const bs = try open(iter.paths_remaining[0]);

        iter.bs = bs;
        iter.paths_remaining = iter.paths_remaining[1..];
        iter.ranges = try bs.scan(ally, iter.size_hint);
        iter.index = 0;
        
        return iter.nextRange();
    }

    /// remember to unmap block when you're done with it
    pub fn next(iter: *Iterator) Error!?Block {
        iter.mutex.lock();
        defer iter.mutex.unlock();

        const range = (try iter.nextRange()) orelse return null;
        return try Block.map(iter.bs.?, range);
    }
};