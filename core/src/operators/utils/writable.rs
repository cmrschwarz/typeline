use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use parking_lot::{
    lock_api::{MappedMutexGuard, MutexGuard},
    RawMutex,
};

pub trait CustomWritableTarget: Send + Sync {
    fn create_writer(&self) -> std::io::Result<AnyWriter>;
    fn create_writer_hide_error(&self) -> AnyWriter {
        match self.create_writer() {
            Ok(w) => w,
            Err(e) => AnyWriter::IoError(Some(e)),
        }
    }
}

pub trait CustomWriter: Send + Sync {
    fn aquire_writer(&self) -> AquiredWriter;
}

pub enum WritableTarget {
    Stdout,
    File(PathBuf),
    Custom(Box<dyn CustomWritableTarget>),
    CustomArc(Arc<dyn CustomWritableTarget>),
}

pub enum AnyWriter<'a> {
    Stdout,
    File(File),
    BufferedFile(BufWriter<File>),
    Custom(Box<dyn CustomWriter + 'a>),
    CustomArc(Arc<dyn CustomWriter + 'a>),
    // option so we can take it and raise it as an error later
    IoError(Option<std::io::Error>),
}

pub enum AquiredWriter<'a> {
    Stdout(std::io::StdoutLock<'a>),
    BufferedStdout(BufWriter<std::io::StdoutLock<'a>>),
    File(&'a mut File),
    BufferedFile(&'a mut BufWriter<File>),
    Custom(&'a mut dyn Write),
    MutexLock(MappedMutexGuard<'a, RawMutex, dyn Write>),
    // option so we can take it and raise it as an error later
    IoError(&'a Option<std::io::Error>),
}

#[derive(Default)]
pub struct MutexedWriteableTargetOwner<W: Write>(
    Arc<MutexedWriteableTarget<W>>,
);

#[derive(Default)]
pub struct MutexedWriteableTarget<W: Write>(pub parking_lot::Mutex<W>);

pub struct MutexedWriter<'a, W: Write>(pub &'a parking_lot::Mutex<W>);

impl<W: Write + Send + 'static> MutexedWriteableTargetOwner<W> {
    pub fn get(&self) -> MutexGuard<RawMutex, W> {
        self.0 .0.lock()
    }
    pub fn create_target(&self) -> WritableTarget {
        WritableTarget::CustomArc(self.0.clone())
    }
}
impl<W: Write + Send + 'static> CustomWritableTarget
    for MutexedWriteableTarget<W>
{
    fn create_writer(&self) -> std::io::Result<AnyWriter> {
        Ok(AnyWriter::Custom(Box::new(MutexedWriter(&self.0))))
    }
}

impl<'a, W: Write + Send + 'static> CustomWritableTarget
    for MutexedWriter<'a, W>
{
    fn create_writer(&self) -> std::io::Result<AnyWriter> {
        Ok(AnyWriter::Custom(Box::new(MutexedWriter(self.0))))
    }
}

impl<'a, W: Write + Send + 'static> CustomWriter for MutexedWriter<'a, W> {
    fn aquire_writer(&self) -> AquiredWriter {
        AquiredWriter::MutexLock(MutexGuard::map(self.0.lock(), |g| {
            g as &mut dyn Write
        }))
    }
}

impl WritableTarget {
    pub fn create_writer(&self, buffered: bool) -> std::io::Result<AnyWriter> {
        match self {
            WritableTarget::Stdout => Ok(AnyWriter::Stdout),
            WritableTarget::File(path) => {
                let f = File::open(path)?;
                Ok(if buffered {
                    AnyWriter::BufferedFile(BufWriter::new(f))
                } else {
                    AnyWriter::File(f)
                })
            }
            WritableTarget::Custom(c) => c.create_writer(),
            WritableTarget::CustomArc(c) => c.create_writer(),
        }
    }
    pub fn create_writer_hide_error(&self, buffered: bool) -> AnyWriter {
        match self.create_writer(buffered) {
            Ok(w) => w,
            Err(e) => AnyWriter::IoError(Some(e)),
        }
    }
}

impl<'a> AnyWriter<'a> {
    pub fn aquire(&mut self, buffered: bool) -> AquiredWriter {
        match self {
            AnyWriter::Stdout => {
                if buffered {
                    AquiredWriter::BufferedStdout(BufWriter::new(
                        std::io::stdout().lock(),
                    ))
                } else {
                    AquiredWriter::Stdout(std::io::stdout().lock())
                }
            }
            AnyWriter::File(f) => AquiredWriter::File(f),
            AnyWriter::BufferedFile(f) => AquiredWriter::BufferedFile(f),
            AnyWriter::Custom(f) => f.aquire_writer(),
            AnyWriter::CustomArc(f) => f.aquire_writer(),
            AnyWriter::IoError(f) => AquiredWriter::IoError(f),
        }
    }
}

impl Write for AquiredWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            AquiredWriter::Stdout(f) => f.write(buf),
            AquiredWriter::BufferedStdout(f) => f.write(buf),
            AquiredWriter::File(f) => f.write(buf),
            AquiredWriter::BufferedFile(f) => f.write(buf),
            AquiredWriter::Custom(f) => f.write(buf),
            AquiredWriter::MutexLock(g) => g.write(buf),
            AquiredWriter::IoError(_) => {
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            AquiredWriter::Stdout(f) => f.flush(),
            AquiredWriter::BufferedStdout(f) => f.flush(),
            AquiredWriter::File(f) => f.flush(),
            AquiredWriter::BufferedFile(f) => f.flush(),
            AquiredWriter::Custom(f) => f.flush(),
            AquiredWriter::MutexLock(g) => g.flush(),
            AquiredWriter::IoError(_) => Ok(()),
        }
    }
}
