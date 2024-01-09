use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Mutex,
};

pub enum WritableTarget {
    Stdout,
    File(PathBuf),
    Custom(Mutex<Option<Box<dyn Write + Send>>>),
}

impl WritableTarget {
    pub fn take_writer(&self, buffered: bool) -> AnyWriter {
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
            WritableTarget::Custom(c) => {
                AnyWriter::Custom(c.lock().unwrap().take().unwrap())
            }
        }
    }
}

pub enum AnyWriter {
    Stdout,
    File(File),
    BufferedFile(BufWriter<File>),
    Custom(Box<dyn Write + Send>),
    // option so we can take it and raise it as an error later
    FileOpenIoError(Option<std::io::Error>),
}

impl AnyWriter {
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
            AnyWriter::Custom(f) => AquiredWriter::Custom(f),
            AnyWriter::FileOpenIoError(f) => AquiredWriter::FileOpenIoError(f),
        }
    }
}

pub enum AquiredWriter<'a> {
    Stdout(std::io::StdoutLock<'a>),
    BufferedStdout(BufWriter<std::io::StdoutLock<'a>>),
    File(&'a mut File),
    BufferedFile(&'a mut BufWriter<File>),
    Custom(&'a mut (dyn Write + Send)),
    // option so we can take it and raise it as an error later
    FileOpenIoError(&'a Option<std::io::Error>),
}

impl Write for AquiredWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            AquiredWriter::Stdout(f) => f.write(buf),
            AquiredWriter::BufferedStdout(f) => f.write(buf),
            AquiredWriter::File(f) => f.write(buf),
            AquiredWriter::BufferedFile(f) => f.write(buf),
            AquiredWriter::Custom(f) => f.write(buf),
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
            AquiredWriter::FileOpenIoError(_) => Ok(()),
        }
    }
}
