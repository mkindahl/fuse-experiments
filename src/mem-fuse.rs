use clap::{crate_version, Arg, Command};
//use daemonize::Daemonize;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate,
    ReplyDirectory, ReplyEntry, ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::{c_int, ENOENT};
use log::{debug, error, LevelFilter};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::prelude::OsStrExt;
use std::path::Path;
use std::str::{from_utf8, Utf8Error};
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

const MAX_NAME_LENGTH: u32 = 255;
const BLOCK_SIZE: u64 = 512;

const CAPTURE_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
    blksize: 512,
};

fn main() {
    let matches = Command::new("PgLogCapture")
        .version(crate_version!())
        .author("Mats Kindahl")
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .value_name("DIR")
                .default_value("/tmp/log-capture")
                .help("Set local directory used to store data")
                .takes_value(true),
        )
        .arg(
            Arg::new("mount")
                .value_name("MOUNT")
                .help("Act as a client, and mount FUSE at given path")
                .takes_value(true),
        )
        .arg(
            Arg::new("params")
                .value_name("PARAMS")
                .help("Database connection parameters")
                .takes_value(true),
        )
        .arg(
            Arg::new("v")
                .short('v')
                .multiple_occurrences(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    // Set up logger
    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(LevelFilter::Debug)
        .init();

    debug!("Setting up logger");
    let mountpoint: String = matches.value_of("mount").unwrap_or_default().to_string();
    debug!("Mountpoint is {}", mountpoint);
    let data_dir: String = matches.value_of("data-dir").unwrap_or_default().to_string();
    debug!("Data directory is {}", data_dir);

    let options = vec![
        MountOption::AllowOther,
        MountOption::RW,
        MountOption::NoExec,
        MountOption::FSName("hello".to_string()),
    ];

    let params: String = matches.value_of("params").unwrap().to_string();
    let filesystem = CaptureFS::new(params, data_dir).unwrap();
    debug!("Filesystem created");

    // let daemonize = Daemonize::new()
    //     .pid_file("/tmp/test.pid")
    //     .working_directory(std::env::current_dir().unwrap().as_path())
    //     .stdout(File::create("/tmp/daemon.out").unwrap())
    //     .stderr(File::create("/tmp/daemon.err").unwrap());

    // match daemonize.start() {
    //     Ok(_) => println!("Success, daemonized"),
    //     Err(e) => eprintln!("Error, {}", e),
    // };

    debug!("Mounting filesystem");
    let result = fuser::mount2(filesystem, mountpoint, &options);
    debug!("Exiting filesystem: {:?}", result);
    if let Err(e) = result {
        // Return a special error code for permission denied, which usually indicates that
        // "user_allow_other" is missing from /etc/fuse.conf
        if e.kind() == ErrorKind::PermissionDenied {
            error!("{}", e.to_string());
            std::process::exit(2);
        }
    }
}

/// This just contain file attributes and data directly.
struct FileData {
    lines: Vec<String>,
    attr: FileAttr,
}

impl FileData {
    fn new(attr: FileAttr) -> FileData {
        let lines = Vec::new();
        FileData { lines, attr }
    }

    fn add_line(&mut self, string: String) {
        self.lines.push(string);
    }
}

/**
 * Structure containing information captured by the file system.
 *
 * The file structure will contain named files that are created and
 * writes to the in-memory entries. The lines will be grouped into
 * records and sent to the database as INSERT statements.
 *
 * The file system is flat, so it is not possible to create
 * directories in the directory, and it can only contain regular files
 * (so this is hard-coded in the code below).
 */
struct CaptureFS {
    data_dir: String,
    last_inode: u64,
    names: HashMap<Vec<u8>, u64>,
    files: BTreeMap<u64, FileData>,
}

impl CaptureFS {
    fn new(_params: String, data_dir: String) -> Result<CaptureFS, postgres::Error> {
        Ok(CaptureFS {
            last_inode: FUSE_ROOT_ID,
            data_dir,
            names: HashMap::new(),
            files: BTreeMap::new(),
        })
    }
}

impl Filesystem for CaptureFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        fs::create_dir_all(Path::new(&self.data_dir).join("inodes")).unwrap();
        fs::create_dir_all(Path::new(&self.data_dir).join("contents")).unwrap();
        Ok(())
    }

    /// Look up the name and return the attributes.
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!(
            "lookup() called with parent={:?} name={:?}",
            parent,
            name.to_os_string().into_string()
        );
        if name.len() > MAX_NAME_LENGTH as usize {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        if parent == FUSE_ROOT_ID {
            if let Some(inode) = self.names.get(name.as_bytes()) {
                if let Some(data) = self.files.get(inode) {
                    reply.entry(&Duration::new(0, 0), &data.attr, 0);
                    return;
                } else {
                    reply.error(libc::EBADFD);
                    return;
                }
            }
        }
        reply.error(ENOENT);
    }

    fn forget(&mut self, _req: &Request, inode: u64, nlookup: u64) {
        debug!(
            "forget() called with inode={:?} nlookup={:?}",
            inode, nlookup
        );
    }

    fn getattr(&mut self, _req: &Request, inode: u64, reply: ReplyAttr) {
        debug!("getattr() called with inode={:?}", inode);
        if inode == FUSE_ROOT_ID {
            reply.attr(&Duration::new(0, 0), &CAPTURE_DIR_ATTR);
        } else if let Some(data) = self.files.get(&inode) {
            reply.attr(&Duration::new(0, 0), &data.attr);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir() called with {:?}", inode);

        // We only allow reading the top directory
        if inode != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }

        for (index, (name, inode)) in self.names.iter().skip(offset as usize).enumerate() {
            let buffer_full: bool = reply.add(
                *inode,
                offset + index as i64 + 1,
                FileType::RegularFile,
                OsStr::from_bytes(name),
            );

            if buffer_full {
                break;
            }
        }
        reply.ok();
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!("create() called with {:?} {:?}", parent, name);
        if parent != FUSE_ROOT_ID {
            reply.error(libc::EBADFD);
            return;
        }

        if self.names.contains_key(name.as_bytes()) {
            reply.error(libc::EEXIST);
            return;
        }

        let (_read, _write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        self.last_inode += 1;
        self.names.insert(name.as_bytes().to_vec(), self.last_inode);
        let data = FileData::new(FileAttr {
            ino: self.last_inode,
            size: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: mode as u16,
            nlink: 0,
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0,
            blocks: 0,
            flags: 0,
            blksize: BLOCK_SIZE as u32,
        });
        reply.created(&Duration::new(0, 0), &data.attr, 0, 0, 0);
        self.files.insert(self.last_inode, data);
    }

    fn write(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        _offset: i64,
        data: &[u8],
        _write_flags: u32,
        #[allow(unused_variables)] flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!(
            "write() called with inode={:?} size={:?}",
            inode,
            data.len()
        );
        if let Some(file_data) = self.files.get_mut(&inode) {
            let lines: Result<Vec<_>, Utf8Error> =
                data.split(|&b| b == b'\n').map(|c| from_utf8(c)).collect();
            for line in lines.unwrap() {
                file_data.add_line(line.to_string())
            }
            reply.written(data.len() as u32);
        } else {
            reply.error(libc::EBADF);
        }
    }
}
