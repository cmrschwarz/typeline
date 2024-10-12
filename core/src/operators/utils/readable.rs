use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use parking_lot::{
    lock_api::{MappedMutexGuard, MutexGuard},
    Mutex, RawMutex,
};

pub trait CustomReadableTarget: Send + Sync {
    fn target_path(&self) -> String;
    fn create_reader(&self) -> std::io::Result<AnyReader>;
}

pub trait CustomBufReadableTarget: CustomReadableTarget {
    fn create_buf_reader(&self) -> std::io::Result<AnyBufReader>;
}

pub trait CustomReader: Send + Sync {
    fn aquire_reader(&self) -> AquiredReader;
}

pub trait CustomBufReader: CustomReader {
    fn aquire_buf_reader(&self) -> AquiredBufReader;
}

// TODO: delete this once std::io::buffered::Buffer is stable
// or add a better implementation
const DEFAULT_BUFFER_SIZE: usize = 8192;
struct Buffer {
    data: Vec<u8>,
    consumed: usize,
}

impl Default for Buffer {
    fn default() -> Self {
        let mut res = Self {
            data: Vec::with_capacity(DEFAULT_BUFFER_SIZE),
            consumed: 0,
        };
        // initialize memory
        unsafe {
            std::ptr::write_bytes(
                res.data.as_mut_ptr(),
                0,
                DEFAULT_BUFFER_SIZE,
            );
        }
        res
    }
}

pub struct CustomReaderBufAdapter<'a, R: Deref<Target = dyn CustomReader + 'a>>
{
    reader: R,
    buffer: Buffer,
}

pub struct AquiredCustomReaderBufAdapter<'a> {
    reader: AquiredReader<'a>,
    buffer: &'a mut Buffer,
}

pub enum ReadableTarget {
    Stdin,
    File(PathBuf),
    Custom(Box<dyn CustomReadableTarget>),
    CustomArc(Arc<dyn CustomReadableTarget>),
    CustomBuf(Box<dyn CustomBufReadableTarget>),
    CustomBufArc(Arc<dyn CustomBufReadableTarget>),
}

pub enum AnyReader<'a> {
    Stdin,
    File(File),
    Custom(Box<dyn CustomReader + 'a>),
    CustomArc(Arc<dyn CustomReader + 'a>),
    CustomBuf(Box<dyn CustomBufReader + 'a>),
    CustomBufArc(Arc<dyn CustomBufReader + 'a>),
    // option so we can take it and raise it as an error later
    IoError(Option<std::io::Error>),
}

pub enum AquiredReader<'a> {
    Stdin(std::io::StdinLock<'a>),
    File(&'a mut File),
    Custom(&'a mut dyn Read),
    MutexLock(MappedMutexGuard<'a, RawMutex, dyn Read>),
    IoError(&'a Option<std::io::Error>),
}

pub enum AnyBufReader<'a> {
    Stdin,
    BufferedFile(BufReader<File>),
    Custom(CustomReaderBufAdapter<'a, Box<dyn CustomReader + 'a>>),
    CustomArc(CustomReaderBufAdapter<'a, Arc<dyn CustomReader + 'a>>),
    CustomBuf(Box<dyn CustomBufReader + 'a>),
    CustomBufArc(Arc<dyn CustomBufReader + 'a>),
    // option so we can take it and raise it as an error later
    IoError(Option<std::io::Error>),
}

pub enum AquiredBufReader<'a> {
    Stdin(std::io::StdinLock<'a>),
    BufferedFile(&'a mut BufReader<File>),
    Custom(&'a mut dyn BufRead),
    MutexLock(MappedMutexGuard<'a, RawMutex, dyn BufRead>),
    IoError(&'a Option<std::io::Error>),
    CustomReaderBufAdapter(AquiredCustomReaderBufAdapter<'a>),
}

pub struct MutexedReadableTargetOwner<W: Read>(Arc<MutexedReadableTarget<W>>);

pub struct MutexedReadableTarget<W: Read>(pub parking_lot::Mutex<W>);

pub struct MutexedReader<'a, W: Read>(pub &'a parking_lot::Mutex<W>);

impl<'a, R: Deref<Target = dyn CustomReader + 'a>>
    CustomReaderBufAdapter<'a, R>
{
    pub fn aquire_buf_reader(&mut self) -> AquiredBufReader {
        match self.reader.aquire_reader() {
            AquiredReader::Stdin(stdin) => AquiredBufReader::Stdin(stdin),
            AquiredReader::IoError(e) => AquiredBufReader::IoError(e),
            reader @ (AquiredReader::Custom(_)
            | AquiredReader::File(_)
            | AquiredReader::MutexLock(_)) => {
                AquiredBufReader::CustomReaderBufAdapter(
                    AquiredCustomReaderBufAdapter {
                        reader,
                        buffer: &mut self.buffer,
                    },
                )
            }
        }
    }
}

pub fn adapt_reader(r: AnyReader) -> AnyBufReader {
    match r {
        AnyReader::Stdin => AnyBufReader::Stdin,
        AnyReader::File(file) => {
            AnyBufReader::BufferedFile(BufReader::new(file))
        }
        AnyReader::Custom(c) => AnyBufReader::Custom(CustomReaderBufAdapter {
            reader: c,
            buffer: Buffer::default(),
        }),
        AnyReader::CustomArc(c) => {
            AnyBufReader::CustomArc(CustomReaderBufAdapter {
                reader: c,
                buffer: Buffer::default(),
            })
        }
        AnyReader::CustomBuf(c) => AnyBufReader::CustomBuf(c),
        AnyReader::CustomBufArc(c) => AnyBufReader::CustomBufArc(c),
        AnyReader::IoError(e) => AnyBufReader::IoError(e),
    }
}

impl<R: Read + Send + 'static> MutexedReadableTargetOwner<R> {
    pub fn new(reader: R) -> Self {
        Self(Arc::new(MutexedReadableTarget(Mutex::new(reader))))
    }
    pub fn get(&self) -> MutexGuard<RawMutex, R> {
        self.0 .0.lock()
    }
    pub fn create_target(&self) -> ReadableTarget {
        ReadableTarget::CustomArc(self.0.clone())
    }
}
impl<R: Read + Send + 'static> CustomReadableTarget
    for MutexedReadableTarget<R>
{
    fn target_path(&self) -> String {
        String::from("<custom readable>")
    }
    fn create_reader(&self) -> std::io::Result<AnyReader> {
        Ok(AnyReader::Custom(Box::new(MutexedReader(&self.0))))
    }
}

impl<'a, W: Read + Send + 'static> CustomReadableTarget
    for MutexedReader<'a, W>
{
    fn target_path(&self) -> String {
        String::from("<custom readable>")
    }
    fn create_reader(&self) -> std::io::Result<AnyReader> {
        Ok(AnyReader::Custom(Box::new(MutexedReader(self.0))))
    }
}

impl<'a, W: Read + Send + 'static> CustomReader for MutexedReader<'a, W> {
    fn aquire_reader(&self) -> AquiredReader {
        AquiredReader::MutexLock(MutexGuard::map(self.0.lock(), |g| {
            g as &mut dyn Read
        }))
    }
}

impl ReadableTarget {
    pub fn target_path(&self) -> String {
        match self {
            ReadableTarget::Stdin => String::from("<stdin>"),
            ReadableTarget::File(path_buf) => {
                path_buf.to_string_lossy().to_string()
            }
            ReadableTarget::Custom(c) => c.target_path(),
            ReadableTarget::CustomArc(c) => c.target_path(),
            ReadableTarget::CustomBuf(c) => c.target_path(),
            ReadableTarget::CustomBufArc(c) => c.target_path(),
        }
    }
    pub fn create_reader(&self) -> std::io::Result<AnyReader> {
        match self {
            ReadableTarget::Stdin => Ok(AnyReader::Stdin),
            ReadableTarget::File(path) => {
                Ok(AnyReader::File(File::open(path)?))
            }
            ReadableTarget::Custom(c) => c.create_reader(),
            ReadableTarget::CustomArc(c) => c.create_reader(),
            ReadableTarget::CustomBuf(c) => c.create_reader(),
            ReadableTarget::CustomBufArc(c) => c.create_reader(),
        }
    }
    pub fn create_reader_hide_error(&self) -> AnyReader {
        match self.create_reader() {
            Ok(r) => r,
            Err(e) => AnyReader::IoError(Some(e)),
        }
    }
    pub fn create_buf_reader(&self) -> std::io::Result<AnyBufReader> {
        match self {
            ReadableTarget::Stdin => Ok(AnyBufReader::Stdin),
            ReadableTarget::File(path) => Ok(AnyBufReader::BufferedFile(
                BufReader::new(File::open(path)?),
            )),
            ReadableTarget::Custom(c) => Ok(adapt_reader(c.create_reader()?)),
            ReadableTarget::CustomArc(c) => {
                Ok(adapt_reader(c.create_reader()?))
            }
            ReadableTarget::CustomBuf(c) => c.create_buf_reader(),
            ReadableTarget::CustomBufArc(c) => c.create_buf_reader(),
        }
    }
    pub fn create_buf_reader_hide_error(&self) -> AnyBufReader {
        match self.create_buf_reader() {
            Ok(r) => r,
            Err(e) => AnyBufReader::IoError(Some(e)),
        }
    }
}

impl<'a> AnyReader<'a> {
    pub fn aquire(&mut self) -> AquiredReader {
        match self {
            AnyReader::Stdin => AquiredReader::Stdin(std::io::stdin().lock()),
            AnyReader::File(f) => AquiredReader::File(f),
            AnyReader::Custom(f) => f.aquire_reader(),
            AnyReader::CustomArc(f) => f.aquire_reader(),
            AnyReader::IoError(f) => AquiredReader::IoError(f),
            AnyReader::CustomBuf(f) => f.aquire_reader(),
            AnyReader::CustomBufArc(f) => f.aquire_reader(),
        }
    }
}

impl Read for AquiredReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            AquiredReader::Stdin(f) => f.read(buf),
            AquiredReader::File(f) => f.read(buf),
            AquiredReader::Custom(f) => f.read(buf),
            AquiredReader::MutexLock(g) => g.read(buf),
            AquiredReader::IoError(_) => {
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }
}

impl<'a> AnyBufReader<'a> {
    pub fn aquire(&mut self) -> AquiredBufReader {
        match self {
            AnyBufReader::Stdin => {
                AquiredBufReader::Stdin(std::io::stdin().lock())
            }
            AnyBufReader::BufferedFile(b) => AquiredBufReader::BufferedFile(b),
            AnyBufReader::Custom(c) => c.aquire_buf_reader(),
            AnyBufReader::CustomArc(c) => c.aquire_buf_reader(),
            AnyBufReader::CustomBuf(c) => c.aquire_buf_reader(),
            AnyBufReader::CustomBufArc(c) => c.aquire_buf_reader(),
            AnyBufReader::IoError(f) => AquiredBufReader::IoError(f),
        }
    }
}

impl Read for AquiredBufReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            AquiredBufReader::Stdin(f) => f.read(buf),
            AquiredBufReader::BufferedFile(f) => f.read(buf),
            AquiredBufReader::Custom(f) => f.read(buf),
            AquiredBufReader::MutexLock(g) => g.read(buf),
            AquiredBufReader::CustomReaderBufAdapter(r) => r.read(buf),
            AquiredBufReader::IoError(_) => {
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }
}

impl BufRead for AquiredBufReader<'_> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            AquiredBufReader::Stdin(f) => f.fill_buf(),
            AquiredBufReader::BufferedFile(f) => f.fill_buf(),
            AquiredBufReader::Custom(f) => f.fill_buf(),
            AquiredBufReader::MutexLock(g) => g.fill_buf(),
            AquiredBufReader::CustomReaderBufAdapter(r) => r.fill_buf(),
            AquiredBufReader::IoError(_) => {
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            AquiredBufReader::Stdin(f) => f.consume(amt),
            AquiredBufReader::BufferedFile(f) => f.consume(amt),
            AquiredBufReader::Custom(f) => f.consume(amt),
            AquiredBufReader::MutexLock(g) => g.consume(amt),
            AquiredBufReader::CustomReaderBufAdapter(r) => r.consume(amt),
            AquiredBufReader::IoError(_) => (),
        }
    }
}

impl<'a> Read for AquiredCustomReaderBufAdapter<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.buffer.data.len() == self.buffer.consumed
            && buf.len() >= self.buffer.data.capacity()
        {
            self.buffer.data.clear();
            self.buffer.consumed = 0;
            return self.reader.read(buf);
        }
        let mut rem = self.fill_buf()?;
        let nread = rem.read(buf)?;
        self.consume(nread);
        Ok(nread)
    }
}

impl<'a> BufRead for AquiredCustomReaderBufAdapter<'a> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        let buffer_len = self.buffer.data.len();
        if buffer_len > self.buffer.consumed {
            return Ok(&self.buffer.data[self.buffer.consumed..]);
        }
        debug_assert!(buffer_len == self.buffer.consumed);
        self.buffer.consumed = 0;
        self.buffer.data.clear();
        unsafe {
            self.buffer.data.set_len(self.buffer.data.capacity());
            let bytes_read = self.reader.read(&mut self.buffer.data);
            match bytes_read {
                Ok(bytes_read) => {
                    self.buffer.data.set_len(bytes_read);
                    Ok(&self.buffer.data)
                }
                Err(e) => {
                    self.buffer.data.set_len(0);
                    Err(e)
                }
            }
        }
    }

    fn consume(&mut self, amt: usize) {
        debug_assert!(amt <= self.buffer.data.len() - self.buffer.consumed);
        self.buffer.consumed += amt;
    }
}
