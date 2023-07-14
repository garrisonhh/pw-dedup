//! stores password data in temporary files so that extrusion is fast and
//! memory is saved

const std = @import("std");
const os = std.os;
const linux = os.linux;
const page_size = std.mem.page_size;
const Allocator = std.mem.Allocator;

const file_size = 64 * page_size;

pub const Error =
    Allocator.Error ||
    os.MMapError ||
    os.MakeDirError ||
    os.TruncateError ||
    os.OpenError ||
    std.fmt.BufPrintError;

pub const Handle = packed struct(u64) {
    file: u32,
    index: u32,
};

pub const File = struct {
    fd: os.fd_t,
    mem: []align(page_size) u8,
    bump: u32 = 0,

    /// allocate memory from a just-created fs file
    fn init(path: []const u8) Error!File {
        // open file
        const fd = try os.open(
            path,
            linux.O.RDWR | linux.O.CREAT,
            linux.S.IRWXU,
        );

        // map memory
        try os.ftruncate(fd, file_size);

        const mem = try os.mmap(
            null,
            file_size,
            linux.PROT.READ | linux.PROT.WRITE,
            linux.MAP.SHARED,
            fd,
            0,
        );

        return .{
            .fd = fd,
            .mem = mem,
        };
    }

    // TODO fn initHuge(str: []const u8) os.MMapError!Self {

    fn deinit(f: File) void {
        os.munmap(f.mem);
        os.close(f.fd);
    }

    fn canStore(f: File, str: []const u8) bool {
        return f.mem.len - f.bump > str.len + 1;
    }

    /// str must fit (use canStore). returns index
    fn store(f: *File, str: []const u8) u32 {
        std.debug.assert(f.canStore(str));

        const index = f.bump;

        @memcpy(f.mem[f.bump..f.bump + str.len], str);
        f.bump += @intCast(u32, str.len);
        f.mem[f.bump] = '\n';
        f.bump += 1;
        
        return index;
    }
};

const Self = @This();

files: std.ArrayListUnmanaged(File) = .{},
dir_path: []const u8,
dir: std.fs.Dir,

pub fn init(temp_dir_path: []const u8) Error!Self {
    const dir = try std.fs.cwd().makeOpenPath(temp_dir_path, .{});

    return Self{
        .dir_path = temp_dir_path,
        .dir = dir,
    };
}

pub fn deinit(self: *Self, ally: Allocator) void {
    for (self.files.items) |file| file.deinit();
    self.files.deinit(ally);
    std.fs.cwd().deleteTree(self.dir_path) catch {};
}

/// writes next temp path to buffer
fn nextPath(self: Self, buf: *[os.PATH_MAX]u8) std.fmt.BufPrintError![]const u8 {
    return std.fmt.bufPrint(
        buf,
        "{s}{d:012}",
        .{self.dir_path, self.files.items.len},
    );
}

pub fn store(self: *Self, ally: Allocator, str: []const u8) Error!Handle {
    if (str.len + 1 > file_size) {
        @panic("TODO store huge");
    }

    if (self.files.items.len == 0 or
        !self.files.items[self.files.items.len - 1].canStore(str))
    {
        var path_buf: [os.PATH_MAX]u8 = undefined;
        const path = try self.nextPath(&path_buf);

        try self.files.append(ally, try File.init(path));
    }

    const fileno = self.files.items.len - 1;
    const index = self.files.items[fileno].store(str);
    
    return .{
        .file = @intCast(u32, fileno),
        .index = index,
    };
}

pub fn get(self: Self, handle: Handle) []const u8 {
    const mem = self.files.items[handle.file].mem;
    const addr = @ptrFromInt([*]const u8, @intFromPtr(mem.ptr) + handle.index);

    var i: usize = 0;    
    while (addr[i] != '\n') i += 1;

    return addr[0..i];
}

pub fn dump(self: Self, writer: anytype) @TypeOf(writer).Error!void {
    std.debug.print("DUMPING\n", .{});

    var progress = std.Progress{};
    const node = progress.start("dumping", self.files.items.len);
    node.activate();
    defer node.end();

    for (self.files.items) |file| {
        const text = file.mem[0..file.bump];
        try writer.writeAll(text);
        node.completeOne();
    }
}