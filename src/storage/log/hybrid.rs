use super::{Range, Scan, Store};
use crate::error::{Error, Result};

use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Display;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek as _, SeekFrom, Write};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

/// A hybrid log store, storing committed entries in an append-only file, uncommitted entries
/// in memory, and metadata in a separate file (should be an on-disk key-value store).
///
/// The log file contains sequential binary log entries, length-prefixed with a big-endian u32.
/// Entries are only flushed to disk when they are committed and permanent, thus the file is
/// written append-only.
///
/// An index of entry positions and sizes is maintained in memory. This is rebuilt on startup by
/// scanning the file, since maintaining the index in a separate file requires additional fsyncing
/// which is expensive. Since datasets are expected to be small, scanning the file on startup is
/// reasonably cheap.
pub struct Hybrid {
    /// The append-only log file. Protected by a mutex for interior mutability (i.e. read seeks).
    file: Mutex<File>,
    /// Index of entry locations and sizes in the log file.
    index: BTreeMap<u64, (u64, u32)>,
    /// Uncommitted log entries.
    uncommitted: VecDeque<Vec<u8>>,
    /// Metadata cache. Flushed to disk on changes.
    metadata: HashMap<Vec<u8>, Vec<u8>>,
    /// The file used to store metadata.
    /// FIXME Should be an on-disk B-tree key-value store.
    metadata_file: File,
    /// If true, fsync writes.
    sync: bool,
}

impl Display for Hybrid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "hybrid")
    }
}

impl Hybrid {
    /// Creates or opens a new hybrid log, with files in the given directory.
    pub fn new(dir: &Path, sync: bool) -> Result<Self> {
        create_dir_all(dir)?;

        let file =
            OpenOptions::new().read(true).write(true).create(true).open(dir.join("raft-log"))?;

        let metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-metadata"))?;

        Ok(Self {
            index: Self::build_index(&file)?,
            file: Mutex::new(file),
            uncommitted: VecDeque::new(),
            metadata: Self::load_metadata(&metadata_file)?,
            metadata_file,
            sync,
        })
    }

    /// 这里将file文件遍历一遍，进行索引，把相关数据解析到index树中
    /// 树中记录的是 key = i = 1/2/3/4... value = (pos: 数据在文件中的位置, size:该数据在文件中占用空间大小)
    /// Builds the index by scanning the log file.
    fn build_index(file: &File) -> Result<BTreeMap<u64, (u64, u32)>> {
        let filesize = file.metadata()?.len();
        let mut bufreader = BufReader::new(file);
        let mut index = BTreeMap::new();
        let mut sizebuf = [0; 4];
        let mut pos = 0;
        let mut i = 1;
        while pos < filesize {
            bufreader.read_exact(&mut sizebuf)?;
            pos += 4;
            let size = u32::from_be_bytes(sizebuf);
            index.insert(i, (pos, size));
            let mut buf = vec![0; size as usize];
            bufreader.read_exact(&mut buf)?;
            pos += size as u64;
            i += 1;
        }
        Ok(index)
    }

    /// Loads metadata from a file.
    /// 使用rust提供的对数据结构体序列化、反序列化方法，将数据存储到文件中
    fn load_metadata(file: &File) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        match bincode::deserialize_from(file) {
            Ok(metadata) => Ok(metadata),
            Err(err) => {
                if let bincode::ErrorKind::Io(err) = &*err {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(HashMap::new());
                    }
                }
                Err(err.into())
            }
        }
    }
}

impl Store for Hybrid {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.uncommitted.push_back(entry);
        Ok(self.len())
    }

    // 将index及之前的log全部commit，并持久化到log file中
    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(Error::Internal(format!("Cannot commit non-existant index {}", index)));
        }
        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot commit below current committed index {}",
                self.index.len() as u64
            )));
        }
        if index == self.index.len() as u64 {
            return Ok(());
        }

        // Mutex<File> 类型，将file加锁，防止多线程同时访问该文件
        // 返回结果 file 可以理解为智能指针，可以对指针进行deref，或者调用方法，直接操作
        let mut file = self.file.lock()?;
        let mut pos = file.seek(SeekFrom::End(0))?;
        let mut bufwriter = BufWriter::new(&mut *file);
        
        // 从self.index 下一个值开始插入数据
        for i in (self.index.len() as u64 + 1)..=index {

            // 从 uncommitted 队列中获取最开头的数据，依次进行提交操作，并将最开头数据移除
            let entry = self
                .uncommitted
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommitted entries".into()))?;

            // 先在文件中写入 entry 的长度数据，存储到4字节中
            bufwriter.write_all(&(entry.len() as u32).to_be_bytes())?;
            pos += 4;
            // 在 index B树中将数据插入
            self.index.insert(i, (pos, entry.len() as u32));

            // 将entry实际内容写入文件
            bufwriter.write_all(&entry)?;
            pos += entry.len() as u64;
        }
        bufwriter.flush()?; // 保证数据全部写入文件
        drop(bufwriter); // 销毁bufwriter
        if self.sync {
            file.sync_data()?;
        }
        Ok(())

        // 在作用域结束后 file 才解锁
    }

    // 返回当前已经commit的log index值
    fn committed(&self) -> u64 {
        self.index.len() as u64
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        match index {
            0 => Ok(None),

            // 判断如果已经被commited，则从Btree中获取在文件中的位置，然后读取文件内容获取
            i if i <= self.index.len() as u64 => {
                let (pos, size) = self.index.get(&i).copied().ok_or_else(|| {
                    Error::Internal(format!("Indexed position not found for entry {}", i))
                })?;
                let mut entry = vec![0; size as usize];
                let mut file = self.file.lock()?;
                file.seek(SeekFrom::Start(pos))?;
                file.read_exact(&mut entry)?;
                Ok(Some(entry))
            }

            // 如果没有被commited，则从uncommitted中获取相关数据
            // uncommitted 中只保存未提交的数据，已经提交的都需要到文件中获取，而相关数据都保存到self.index 树中
            i => Ok(self.uncommitted.get(i as usize - self.index.len() - 1).cloned()),
        }
    }

    fn len(&self) -> u64 {
        self.index.len() as u64 + self.uncommitted.len() as u64
    }

    // range 标识需要scan的范围，简单点说，就是从 n--m，从编号n到编号m的范围，也可以指定n,m是否在范围内
    fn scan(&self, range: Range) -> Scan {
        let start = match range.start {
            Bound::Included(0) => 1,
            Bound::Included(n) => n,
            Bound::Excluded(n) => n + 1,
            Bound::Unbounded => 1,
        };
        let end = match range.end {
            Bound::Included(n) => n,
            Bound::Excluded(0) => 0,
            Bound::Excluded(n) => n - 1,
            Bound::Unbounded => self.len(),
        };

        let mut scan: Scan = Box::new(std::iter::empty());
        if start > end {
            return scan;
        }

        // Scan committed entries in file
        if let Some((offset, _)) = self.index.get(&start) {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(*offset - 4)).unwrap(); // seek to length prefix
            let mut bufreader = BufReader::new(MutexReader(file)); // FIXME Avoid MutexReader
            scan =
                Box::new(scan.chain(self.index.range(start..=end).map(move |(_, (_, size))| {
                    let mut sizebuf = vec![0; 4];
                    bufreader.read_exact(&mut sizebuf)?;
                    let mut entry = vec![0; *size as usize];
                    bufreader.read_exact(&mut entry)?;
                    Ok(entry)
                })));
        }

        // Scan uncommitted entries in memory
        if end > self.index.len() as u64 {
            scan = Box::new(
                scan.chain(
                    self.uncommitted
                        .iter()
                        // skip 跳过前几个
                        .skip(start as usize - min(start as usize, self.index.len() + 1))
                        // take 获取几个元素
                        .take(end as usize - max(start as usize, self.index.len()) + 1)
                        .cloned() // 这里clone后，iterator中每个item类型是Vec<>，而返回类型item是 Result<Vec<>>
                        .map(Ok),// 所以把每个item都做一个Ok操作，变成Result类型
                ),
            )
        }

        scan
    }

    fn size(&self) -> u64 {
        self.index.iter().next_back().map(|(_, (pos, size))| *pos + *size as u64).unwrap_or(0)
    }

    // truncate删除index以后的所有数据
    fn truncate(&mut self, index: u64) -> Result<u64> {
        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot truncate below committed index {}",
                self.index.len() as u64
            )));
        }
        self.uncommitted.truncate(index as usize - self.index.len());
        Ok(self.len())
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.metadata.insert(key.to_vec(), value);

        // 将文件内容全部删除，重新写入数据
        self.metadata_file.set_len(0)?;
        self.metadata_file.seek(SeekFrom::Start(0))?;
        bincode::serialize_into(&mut self.metadata_file, &self.metadata)?;
        if self.sync {
            self.metadata_file.sync_data()?;
        }
        Ok(())
    }
}

impl Drop for Hybrid {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.metadata_file.sync_all().ok();
        self.file.lock().map(|f| f.sync_all()).ok();
    }
}

struct MutexReader<'a>(MutexGuard<'a, File>);

impl<'a> Read for MutexReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

#[cfg(test)]
impl super::TestSuite<Hybrid> for Hybrid {
    fn setup() -> Result<Self> {
        let dir = tempdir::TempDir::new("toydb")?;
        Hybrid::new(dir.as_ref(), false)
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    Hybrid::test()
}

#[test]
fn test_persistent() -> Result<()> {
    let dir = tempdir::TempDir::new("toydb")?;
    let mut l = Hybrid::new(dir.as_ref(), true)?;

    l.append(vec![0x01])?;
    l.append(vec![0x02])?;
    l.append(vec![0x03])?;
    l.append(vec![0x04])?;
    l.append(vec![0x05])?;
    l.commit(3)?;

    let mut l = Hybrid::new(dir.as_ref(), true)?;
    l.append(vec![0x04])?;

    assert_eq!(
        vec![vec![1], vec![2], vec![3], vec![4]],
        l.scan(Range::from(..)).collect::<Result<Vec<_>>>()?
    );

    Ok(())
}
