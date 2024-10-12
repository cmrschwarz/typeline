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
    fn create_writer(&self) -> AnyWriter;
}

pub trait CustomWriter<'a>: Send + Sync {
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
    Custom(Box<dyn CustomWriter<'a> + 'a>),
    CustomArc(Arc<dyn CustomWriter<'a> + 'a>),
    // option so we can take it and raise it as an error later
    FileOpenIoError(Option<std::io::Error>),
}

pub enum AquiredWriter<'a> {
    Stdout(std::io::StdoutLock<'a>),
    BufferedStdout(BufWriter<std::io::StdoutLock<'a>>),
    File(&'a mut File),
    BufferedFile(&'a mut BufWriter<File>),
    Custom(&'a mut dyn Write),
    MutexLock(MappedMutexGuard<'a, RawMutex, dyn Write>),
    // option so we can take it and raise it as an error later
    FileOpenIoError(&'a Option<std::io::Error>),
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
    fn create_writer(&self) -> AnyWriter {
        AnyWriter::Custom(Box::new(MutexedWriter(&self.0)))
    }
}

impl<'a, W: Write + Send + 'static> CustomWritableTarget
    for MutexedWriter<'a, W>
{
    fn create_writer(&self) -> AnyWriter {
        AnyWriter::Custom(Box::new(MutexedWriter(self.0)))
    }
}

impl<'a, W: Write + Send + 'static> CustomWriter<'a> for MutexedWriter<'a, W> {
    fn aquire_writer(&self) -> AquiredWriter {
        AquiredWriter::MutexLock(MutexGuard::map(self.0.lock(), |g| {
            g as &mut dyn Write
        }))
    }
}

impl WritableTarget {
    pub fn create_writer(&self, buffered: bool) -> AnyWriter {
        match self {
            WritableTarget::Stdout => AnyWriter::Stdout,
            WritableTarget::File(path) => match File::open(path) {
                Ok(f) => {
                    if buffered {
                        AnyWriter::BufferedFile(BufWriter::new(f))
                    } else {
                        AnyWriter::File(f)
                    }
                }
                Err(e) => AnyWriter::FileOpenIoError(Some(e)),
            },
            WritableTarget::Custom(c) => c.create_writer(),
            WritableTarget::CustomArc(c) => c.create_writer(),
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
            AnyWriter::FileOpenIoError(f) => AquiredWriter::FileOpenIoError(f),
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
            AquiredWriter::FileOpenIoError(_) => {
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
            AquiredWriter::FileOpenIoError(_) => Ok(()),
        }
    }
}
