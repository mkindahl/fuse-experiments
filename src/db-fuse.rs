use clap::{crate_version, Arg, Command};
use daemonize::Daemonize;
use fuser::TimeOrNow;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::{c_int, ENOENT};
use log::{debug, error, LevelFilter};
use postgres::Statement;
use postgres::{Client, NoTls};
use std::ffi::OsStr;
use std::fs::File;
use std::io::ErrorKind;
use std::str::{from_utf8, Utf8Error};
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

const MAX_NAME_LENGTH: u32 = 255;
const BLOCK_SIZE: u64 = 512;
const ZERO: Duration = Duration::new(0, 0);

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
    let matches = Command::new("Database FUSE")
        .version(crate_version!())
        .author("Mats Kindahl")
        .arg(
            Arg::new("mount")
                .value_name("MOUNT")
                .help("Act as a client, and mount FUSE at given path")
                .takes_value(true),
        )
        .arg(
            Arg::new("daemonize")
                .short('d')
                .required(false)
                .takes_value(false)
                .help("Daemonize the FUSE process"),
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
    let verbosity: u64 = matches.occurrences_of("v");
    let log_level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(log_level)
        .init();

    debug!("Setting up logger");
    let mountpoint: String = matches.value_of("mount").unwrap_or_default().to_string();
    debug!("Mountpoint is {}", mountpoint);

    let options = vec![
        MountOption::AllowOther,
        MountOption::RW,
        MountOption::NoExec,
        MountOption::FSName("hello".to_string()),
    ];

    let params: String = matches.value_of("params").unwrap().to_string();
    let filesystem = DatabaseFS::new(params).unwrap();
    debug!("Database connection established");

    if matches.is_present("daemonize") {
        let daemonize = Daemonize::new()
            .pid_file("/tmp/db-fuse.pid")
            .working_directory(std::env::current_dir().unwrap().as_path())
            .stdout(File::create("/tmp/db-fuse.out").unwrap())
            .stderr(File::create("/tmp/db-fuse.err").unwrap());

        match daemonize.start() {
            Ok(_) => println!("Success, daemonized"),
            Err(e) => eprintln!("Error, {}", e),
        };
    }

    let result = fuser::mount2(filesystem, mountpoint, &options);
    if let Err(e) = result {
        // Return a special error code for permission denied, which usually indicates that
        // "user_allow_other" is missing from /etc/fuse.conf
        if e.kind() == ErrorKind::PermissionDenied {
            error!("{}", e.to_string());
            std::process::exit(2);
        }
    }
}

fn new_attr(ino: i64, uid: u32, gid: u32, mode: u32) -> FileAttr {
    FileAttr {
        ino: ino as u64,
        size: 0,
        atime: SystemTime::now(),
        mtime: SystemTime::now(),
        ctime: SystemTime::now(),
        crtime: SystemTime::UNIX_EPOCH,
        kind: FileType::RegularFile,
        perm: mode as u16,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blocks: 0,
        flags: 0,
        blksize: BLOCK_SIZE as u32,
    }
}

/**
 * Structure containing information captured by the file system.
 *
 * The lines will sent to the database as INSERT statements and there
 * is a set of function available that interfaces between the database
 * and FUSE.
 *
 * The function in the database file system accepts FUSE types, but
 * these needs to be translated to suitable database types for
 * storage.
 */
struct DatabaseFS {
    client: Client,
    entries: Option<Vec<postgres::Row>>,
    name_lookup: Statement,
    content_insert: Statement,
    inode_lookup: Statement,
    inode_insert: Statement,
    directory_scan: Statement,
}

impl Drop for DatabaseFS {
    fn drop(&mut self) {
        self.client.execute("DROP TABLE inodes", &[]).unwrap();
        self.client.execute("DROP TABLE content", &[]).unwrap();
    }
}

impl DatabaseFS {
    fn new(params: String) -> Result<DatabaseFS, postgres::Error> {
        let mut client = Client::connect(&params, NoTls)?;
        client.execute(
            "CREATE TABLE inodes (ino serial, name name, mode int, uid int, gid int)",
            &[],
        )?;
        client.execute(
            "ALTER SEQUENCE inodes_ino_seq MINVALUE 10 START 10 RESTART",
            &[],
        )?;
        client.execute("CREATE TABLE content (ino int, line text)", &[])?;

        let entries = None;
        let name_lookup =
            client.prepare("SELECT ino, uid, gid, mode FROM inodes WHERE name = $1")?;
        let inode_lookup =
            client.prepare("SELECT ino, uid, gid, mode FROM inodes WHERE ino = $1")?;
        let content_insert = client.prepare("INSERT INTO content(ino, line) VALUES ($1,$2)")?;
        let inode_insert = client.prepare(
            "INSERT INTO inodes(name, mode, uid, gid) VALUES ($1, $2, $3, $4) RETURNING ino",
        )?;
        let directory_scan = client.prepare("SELECT name, ino FROM inodes ORDER BY ino")?;

        Ok(DatabaseFS {
            client,
            entries,
            name_lookup,
            content_insert,
            inode_lookup,
            inode_insert,
            directory_scan,
        })
    }

    fn lookup_name(&mut self, name: &str) -> Result<FileAttr, postgres::Error> {
        let row = self.client.query_one(&self.name_lookup, &[&name])?;
        let ino: i32 = row.get("ino");
        let uid: i32 = row.get("uid");
        let gid: i32 = row.get("gid");
        let mode: i32 = row.get("mode");
        let attr = new_attr(ino as i64, uid as u32, gid as u32, mode as u32);
        debug!("found name {:?}: {:?}", name, attr);
        Ok(attr)
    }

    fn get_inode(&mut self, ino: u64) -> Result<FileAttr, c_int> {
        let ino = ino as i32;
        let result = self.client.query_one(&self.inode_lookup, &[&ino]);
        let row = match result {
            Ok(row) => row,
            Err(err) => {
                debug!("query error: {}", err);
                return Err(libc::ENOENT);
            }
        };
        let ino: i32 = row.get("ino");
        let uid: i32 = row.get("uid");
        let gid: i32 = row.get("gid");
        let mode: i32 = row.get("mode");
        let attr = new_attr(ino as i64, uid as u32, gid as u32, mode as u32);
        debug!("found inode {}: {:?}", ino, attr);
        Ok(attr)
    }

    fn allocate_inode(
        &mut self,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, postgres::Error> {
        let ino: i32 = {
            let mode = mode as i32;
            let uid = uid as i32;
            let gid = gid as i32;
            let row = self
                .client
                .query_one(&self.inode_insert, &[&name, &mode, &uid, &gid])?;
            row.get("ino")
        };
        Ok(new_attr(ino as i64, uid, gid, mode))
    }

    // Data is split up into lines and written to the content table.
    fn write_inode(&mut self, ino: i32, data: &[u8]) -> Result<(), postgres::Error> {
        let ino = ino as i32;
        let lines: Result<Vec<_>, Utf8Error> = data
            .split(|&b| b == b'\n')
            .filter_map(|c| {
                if c.len() > 0 {
                    Some(from_utf8(c))
                } else {
                    None
                }
            })
            .collect();
        for line in lines.unwrap() {
            self.client.execute(&self.content_insert, &[&ino, &line])?;
        }
        Ok(())
    }
}

impl Filesystem for DatabaseFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        Ok(())
    }

    /// Look up the name and return the attributes.
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if name.len() > MAX_NAME_LENGTH as usize {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        if parent == FUSE_ROOT_ID {
            if let Ok(attrs) = self.lookup_name(name.to_str().unwrap()) {
                reply.entry(&ZERO, &attrs, 0);
            } else {
                reply.error(libc::ENOENT);
            }
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn forget(&mut self, _req: &Request, _inode: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &Request, inode: u64, reply: ReplyAttr) {
        if inode == FUSE_ROOT_ID {
            reply.attr(&ZERO, &CAPTURE_DIR_ATTR);
        } else if let Ok(attrs) = self.get_inode(inode) {
            reply.attr(&ZERO, &attrs);
        } else {
            reply.error(ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let mut attrs = match self.get_inode(inode) {
            Ok(attrs) => attrs,
            Err(error_code) => {
                reply.error(error_code);
                return;
            }
        };

        // This is chmod()
        if let Some(mode) = mode {
            debug!("setting mode: ino={} mode={:?}", inode, mode);
            // TODO: Check permission
            attrs.perm = mode as u16;
            let result = self.client.execute(
                "UPDATE inodes SET mode = $1 WHERE ino = $2",
                &[&mode, &(inode as i32)],
            );
            if let Err(_) = result {
                reply.error(libc::EINVAL);
                return;
            }
        }

        // This is chown()
        if let Some(gid) = gid {
            debug!("setting gid: ino={} gid={:?}", inode, gid);
            attrs.gid = gid;
            let result = self.client.execute(
                "UPDATE inodes SET gid = $1 WHERE ino = $2",
                &[&gid, &(inode as i32)],
            );
            if let Err(_) = result {
                reply.error(libc::EINVAL);
                return;
            }
        }

        if let Some(uid) = uid {
            debug!("setting uid: ino={} uid={:?}", inode, uid);
            attrs.uid = uid;
            let result = self.client.execute(
                "UPDATE inodes SET uid = $1 WHERE ino = $2",
                &[&uid, &(inode as i32)],
            );
            if let Err(_) = result {
                reply.error(libc::EINVAL);
                return;
            }
        }

        // This is truncate()
        if let Some(size) = size {
            debug!("setting size: ino={} size={:?}", inode, size);
            reply.error(libc::EPERM);
            return;
        }

        if let Some(atime) = atime {
            debug!("setting atime: ino={} atime={:?}", inode, atime);
            // Does not do anything right now.
            attrs.atime = match atime {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => SystemTime::now(),
            }
        }

        if let Some(mtime) = mtime {
            // Does not do anything right now.
            debug!("setting mtime: ino={} mtime={:?}", inode, mtime);
            attrs.mtime = match mtime {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => SystemTime::now(),
            }
        }

        reply.attr(&ZERO, &attrs);
    }

    fn opendir(&mut self, _req: &Request, inode: u64, _flags: i32, reply: ReplyOpen) {
        debug!("opendir() called with {:?}", inode);

        // We only allow reading the top directory
        if inode != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }

        let result = self.client.query(&self.directory_scan, &[]);

        match result {
            Ok(files) => {
                self.entries = Some(files);
                reply.opened(42, 0);
            }
            Err(_) => reply.error(libc::EBADF),
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        debug!("releasedir() called with ino={} fh={}", ino, fh);
        self.entries = None;
        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request,
        inode: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir() called with fh={} ino={}", fh, inode);

        // We only allow reading the top directory
        if fh != 42 {
            reply.error(libc::EINVAL);
            return;
        }

        // Need to handle the case that the buffer can be full, but we
        // ignore that now.
        if let Some(entries) = self.entries.take() {
            for (index, row) in entries.iter().enumerate() {
                let name: &str = row.get("name");
                let ino: i32 = row.get("ino");
                let _ = reply.add(
                    ino as u64,
                    offset + index as i64,
                    FileType::RegularFile,
                    name,
                );
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
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if parent != FUSE_ROOT_ID {
            reply.error(libc::EBADFD);
        } else if let Ok(_) = self.lookup_name(name.to_str().unwrap()) {
            reply.error(libc::EEXIST);
        } else {
            match self.allocate_inode(name.to_str().unwrap(), req.uid(), req.gid(), mode) {
                Ok(attrs) => {
                    reply.created(&ZERO, &attrs, 0, 0, 0);
                }
                Err(err) => {
                    debug!("query error {}", err);
                    reply.error(libc::EBADFD);
                }
            }
        }
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
        match self.write_inode(inode as i32, data) {
            Ok(_) => reply.written(data.len() as u32),
            Err(err) => {
                debug!("query error: {}", err);
                reply.error(libc::EBADF);
            }
        }
    }
}
