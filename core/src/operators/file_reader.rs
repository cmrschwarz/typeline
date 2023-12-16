use std::{
    ffi::OsStr,
    fs::File,
    io::{stdin, BufRead, BufReader, ErrorKind, IsTerminal, Read, Write},
    path::PathBuf,
    sync::Mutex,
};

use bstr::ByteSlice;
use regex::Regex;
use smallstr::SmallString;

use crate::{
    chain::{BufferingMode, Chain},
    job_session::JobData,
    options::argument::CliArgIdx,
    record_data::{
        field_data::{
            field_value_flags, FieldValueFormat, FieldValueRepr,
            FieldValueSize, INLINE_STR_MAX_LEN,
        },
        push_interface::PushInterface,
        stream_value::{StreamValue, StreamValueData, StreamValueId},
    },
};

use super::{
    errors::{
        io_error_to_op_error, OperatorCreationError, OperatorSetupError,
    },
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
};

pub enum FileKind {
    Stdin,
    File(PathBuf),
    Custom(Mutex<Option<Box<dyn Read + Send>>>),
}

pub enum AnyFile {
    Stdin, // we can't hold the lock here because that wouldn't be Send...
    File(File),
    BufferedFile(BufReader<File>),
    Custom(Box<dyn Read + Send>),
    // option so we can take it and raise it as an error later
    FileOpenIoError(Option<std::io::Error>),
}

#[derive(Clone)]
enum LineBufferedSetting {
    Yes,
    No,
    IfTTY,
}

pub struct OpFileReader {
    file_kind: FileKind,
    line_buffered: LineBufferedSetting,
    insert_count: Option<usize>,
}

impl OpFileReader {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        let mut res = SmallString::new();
        match self.file_kind {
            FileKind::Stdin => res.push_str("stdin"),
            FileKind::File(_) => res.push_str("file"),
            FileKind::Custom(_) => res.push_str("<custom_file_stream>"),
        }
        res
    }
}

pub struct TfFileReader {
    // in case of errors, we close this by take()ing the file, therefore
    // option
    file: Option<AnyFile>,
    stream_value: Option<StreamValueId>,
    value_committed: bool,
    stream_value_committed: bool,
    line_buffered: bool,
    stream_buffer_size: usize,
    stream_size_threshold: usize,
    insert_count: Option<usize>,
}

pub fn build_tf_file_reader<'a>(
    sess: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFileReader,
    tf_state: &TransformState,
) -> TransformData<'a> {
    // TODO: properly set up line buffering
    let mut check_if_tty = false;
    let mut line_buffered = match op.line_buffered {
        LineBufferedSetting::Yes => true,
        LineBufferedSetting::No => false,
        LineBufferedSetting::IfTTY => {
            check_if_tty = true;
            false
        }
    };
    let file = match &op.file_kind {
        FileKind::Stdin => {
            let stdin = std::io::stdin().lock();
            if check_if_tty {
                line_buffered = stdin.is_terminal();
            }
            AnyFile::Stdin
        }
        FileKind::File(path) => match File::open(path) {
            Ok(f) => {
                if check_if_tty && f.is_terminal() {
                    line_buffered = true;
                    AnyFile::BufferedFile(BufReader::new(f))
                } else {
                    AnyFile::File(f)
                }
            }
            Err(e) => AnyFile::FileOpenIoError(Some(e)),
        },
        FileKind::Custom(reader) => {
            AnyFile::Custom(reader
                .lock()
                .unwrap()
                .take()
                .expect("attempted to create two transforms from a single custom FileKind"))
        }
    };
    let chain_settings =
        &sess.get_transform_chain_from_tf_state(tf_state).settings;
    TransformData::FileReader(TfFileReader {
        file: Some(file),
        stream_value: None,
        line_buffered,
        stream_size_threshold: chain_settings.stream_size_threshold,
        stream_buffer_size: chain_settings.stream_buffer_size,
        insert_count: op.insert_count,
        value_committed: false,
        stream_value_committed: false,
    })
}

fn read_size_limited<F: Read>(
    f: &mut F,
    limit: usize,
    target: &mut impl Write,
) -> Result<(usize, bool), std::io::Error> {
    let size = std::io::copy(&mut f.take(limit as u64), target)? as usize;
    let eof = size != limit;
    Ok((size, eof))
}
fn read_line_buffered<F: BufRead>(
    f: &mut F,
    limit: usize,
    target: &mut impl Write,
) -> Result<(usize, bool), std::io::Error> {
    let mut r = f.take(limit as u64);
    let mut read = 0;
    loop {
        let (done, used) = {
            let available = match r.fill_buf() {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            match memchr::memchr(b'\n', available) {
                Some(i) => {
                    target.write_all(&available[..=i])?;
                    (true, i + 1)
                }
                None => {
                    target.write_all(available)?;
                    (false, available.len())
                }
            }
        };
        r.consume(used);
        read += used;
        if done || used == 0 {
            return Ok((read, used == 0));
        }
    }
}
fn read_mode_based<F: BufRead>(
    f: &mut F,
    limit: usize,
    target: &mut impl Write,
    line_buffered: bool,
) -> Result<(usize, bool), std::io::Error> {
    if line_buffered {
        read_line_buffered(f, limit, target)
    } else {
        read_size_limited(f, limit, target)
    }
}

fn read_chunk(
    target: &mut impl Write,
    file: &mut AnyFile,
    limit: usize,
    line_buffered: bool,
) -> Result<(usize, bool), std::io::Error> {
    let (size, eof) = match file {
        AnyFile::BufferedFile(f) => {
            read_mode_based(f, limit, target, line_buffered)
        }
        AnyFile::Stdin => {
            read_mode_based(&mut stdin().lock(), limit, target, line_buffered)
        }
        AnyFile::File(f) => read_size_limited(f, limit, target),
        AnyFile::FileOpenIoError(e) => Err(e.take().unwrap()),
        AnyFile::Custom(r) => read_size_limited(r, limit, target),
    }?;
    Ok((size, eof))
}

fn start_streaming_file(
    sess: &mut JobData,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) {
    let of_id = sess.tf_mgr.prepare_output_field(
        &mut sess.field_mgr,
        &mut sess.match_set_mgr,
        tf_id,
    );

    let mut output_field = sess.field_mgr.fields[of_id].borrow_mut();
    // we want to write the chunk straight into field data to avoid a copy
    // SAFETY: this relies on the memory layout in field_data.
    // since that is a submodule of us, this is fine.
    // ideally though, FieldData would expose some way to do this safely.
    let fdi = unsafe { output_field.iter_hall.internals() };

    let size_before = fdi.data.len();
    let res = read_chunk(
        fdi.data,
        fr.file.as_mut().unwrap(),
        INLINE_STR_MAX_LEN
            .min(fr.stream_buffer_size)
            .min(fr.stream_size_threshold),
        fr.line_buffered,
    );
    let chunk_size = match res {
        Ok((size, eof)) => {
            if eof {
                fr.file.take();
                *fdi.field_count += 1;
                unsafe {
                    output_field.iter_hall.raw().add_header_for_single_value(
                        FieldValueFormat {
                            repr: FieldValueRepr::BytesInline,
                            flags: field_value_flags::SHARED_VALUE,
                            size: size as FieldValueSize,
                        },
                        1,
                        false,
                        false,
                    );
                }
                return;
            }
            size
        }
        Err(e) => {
            fdi.data.truncate(size_before);
            let err = io_error_to_op_error(
                sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                e,
            );
            output_field.iter_hall.push_error(err, 1, false, false);
            fr.file.take();
            return;
        }
    };
    let mut buf = Vec::with_capacity(fr.stream_buffer_size);
    buf.extend_from_slice(&fdi.data[size_before..(size_before + chunk_size)]);
    fdi.data.truncate(size_before);
    let mut done = false;
    let buf_len = buf.len();
    if buf_len < fr.stream_buffer_size {
        match read_chunk(
            &mut buf,
            fr.file.as_mut().unwrap(),
            fr.stream_buffer_size - buf_len,
            fr.line_buffered,
        ) {
            Ok((_size, eof)) => {
                done = eof;
                if eof {
                    fr.file.take();
                }
            }
            Err(e) => {
                let err = io_error_to_op_error(
                    sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                    e,
                );
                output_field.iter_hall.push_error(err, 1, false, false);
                fr.file.take();
                return;
            }
        }
    };
    let sv_id = sess.sv_mgr.stream_values.claim_with_value(StreamValue {
        data: StreamValueData::Bytes(buf),
        done,
        ref_count: 1,
        bytes_are_utf8: false,
        bytes_are_chunk: true,
        subscribers: Default::default(),
    });
    fr.stream_value = Some(sv_id);
    output_field
        .iter_hall
        .push_stream_value_id(sv_id, 1, false, false);
    drop(output_field);
    // even if the stream is already done, we can only drop the stream value
    // next time we are called once it was observed -> refcounted
    sess.tf_mgr.make_stream_producer(tf_id);
}

pub fn handle_tf_file_reader_stream(
    sess: &mut JobData,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) {
    let sv_id = fr.stream_value.unwrap();
    let Some(file) = fr.file.as_mut() else {
        // this happens if the stream is immediately done,
        // but we had to wait for subscribers before unregistering it
        sess.sv_mgr.drop_field_value_subscription(sv_id, None);
        return;
    };
    let mut need_buffering = false;
    if !fr.stream_value_committed {
        fr.stream_value_committed = true;
        let tf = &sess.tf_mgr.transforms[tf_id];
        // if input is not done, subsequent records might need the stream
        if !tf.input_is_done {
            need_buffering = true;
        } else {
            let input_field = sess.field_mgr.fields[tf.input_field].borrow();
            // some fork variant / etc. might still need this value later
            if input_field.get_clear_delay_request_count() != 0 {
                need_buffering = true;
            }
        }
    }
    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    if need_buffering {
        sv.bytes_are_chunk = false;
    }

    let res = match &mut sv.data {
        StreamValueData::Bytes(ref mut bc) => {
            if sv.bytes_are_chunk {
                bc.clear();
            }
            read_chunk(bc, file, fr.stream_buffer_size, fr.line_buffered)
        }
        StreamValueData::Error(_) => Ok((0, true)),
        StreamValueData::Dropped => {
            panic!("dropped stream value ovserved")
        }
    };
    let done = match res {
        Ok((_size, eof)) => eof,
        Err(err) => {
            let err = io_error_to_op_error(
                sess.tf_mgr.transforms[tf_id].op_id.unwrap(),
                err,
            );
            sv.data = StreamValueData::Error(err);
            true
        }
    };
    sv.done = done;
    sess.sv_mgr.inform_stream_value_subscribers(sv_id);
    if done {
        fr.file.take();
        // we only drop our stream value once all input is already handled
        if sess.tf_mgr.transforms[tf_id].mark_for_removal {
            sess.sv_mgr.drop_field_value_subscription(sv_id, None);
        }
        return;
    }
    sess.tf_mgr.make_stream_producer(tf_id);
}

pub fn handle_tf_file_reader(
    sess: &mut JobData,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) {
    let initial_call = !fr.value_committed;
    if initial_call {
        fr.value_committed = true;
        start_streaming_file(sess, tf_id, fr);
    }
    let (batch_size, input_done) = sess.tf_mgr.maintain_single_value(
        tf_id,
        &mut fr.insert_count,
        &sess.field_mgr,
        &mut sess.match_set_mgr,
        initial_call,
        true,
    );

    if input_done {
        sess.unlink_transform(tf_id, batch_size);
    } else {
        sess.tf_mgr.update_ready_state(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, batch_size);
    }
}

lazy_static::lazy_static! {
    static ref ARG_REGEX: Regex = Regex::new(r"^(?<kind>~bytes|~str|file|stdin)(?<insert_count>[0-9]+)?$").unwrap();
}

pub fn argument_matches_op_file_reader(arg: &str) -> bool {
    ARG_REGEX.is_match(arg)
}

pub fn parse_op_file_reader(
    argument: &str,
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let args = ARG_REGEX.captures(argument).ok_or_else(|| {
        // can't happen from cli because of argument_matches_op_file_reader
        OperatorCreationError::new(
            "invalid argument syntax for file reader",
            arg_idx,
        )
    })?;
    let insert_count = args
        .name("insert_count")
        .map(|ic| {
            ic.as_str().parse::<usize>().map_err(|_| {
                OperatorCreationError::new(
                    "failed to parse insertion count as integer",
                    arg_idx,
                )
            })
        })
        .transpose()?;
    match args.name("kind").unwrap().as_str() {
        "file" => parse_op_file(value, insert_count, arg_idx),
        "stdin" => parse_op_stdin(value, insert_count, arg_idx),
        "~str" => parse_op_stream_str(value, insert_count, arg_idx),
        "~bytes" => parse_op_stream_bytes(value, insert_count, arg_idx),
        _ => unreachable!(),
    }
}

pub fn parse_op_file(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let path = if let Some(value) = value {
        #[cfg(unix)]
        {
            PathBuf::from(
                <OsStr as std::os::unix::prelude::OsStrExt>::from_bytes(value),
            )
        }
        #[cfg(windows)]
        {
            PathBuf::from(value.to_str().map_err(|_| {
                OperatorCreationError::new(
                    "failed to parse file path argument as unicode",
                    arg_idx,
                )
            })?)
        }
    } else {
        return Err(OperatorCreationError::new(
            "missing path argument for file",
            arg_idx,
        ));
    };
    Ok(create_op_file(path, insert_count.unwrap_or(0)))
}

pub fn parse_op_stdin(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "stdin does not take arguments",
            arg_idx,
        ));
    };
    Ok(create_op_stdin(insert_count.unwrap_or(0)))
}

pub fn parse_op_stream_str(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new("missing value for ~str", arg_idx)
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new(
                "~str argument must be valid UTF-8, consider using ~bytes=...",
                arg_idx,
            )
        })?;
    Ok(create_op_stream_str(value_str, insert_count.unwrap_or(0)))
}

pub fn parse_op_stream_bytes(
    value: Option<&[u8]>,
    insert_count: Option<usize>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_bytes = value.ok_or_else(|| {
        OperatorCreationError::new("missing value for ~bytes", arg_idx)
    })?;
    Ok(create_op_stream_bytes(
        value_bytes,
        insert_count.unwrap_or(0),
    ))
}

// insert count 0 means all input will be consumed
pub fn create_op_file_reader(
    file_kind: FileKind,
    insert_count: usize,
) -> OperatorData {
    OperatorData::FileReader(OpFileReader {
        file_kind,
        insert_count: if insert_count == 0 {
            None
        } else {
            Some(insert_count)
        },
        // will be set during setup_op_file_reader
        line_buffered: LineBufferedSetting::No,
    })
}

pub fn create_op_file(path: PathBuf, insert_count: usize) -> OperatorData {
    create_op_file_reader(FileKind::File(path), insert_count)
}
pub fn create_op_stdin(insert_count: usize) -> OperatorData {
    create_op_file_reader(FileKind::Stdin, insert_count)
}

pub fn create_op_stream_str(value: &str, insert_count: usize) -> OperatorData {
    create_op_file_reader_custom(
        Box::new(BytesReader::from_string(value.to_owned())),
        insert_count,
    )
}
pub fn create_op_stream_bytes(
    value: &[u8],
    insert_count: usize,
) -> OperatorData {
    create_op_file_reader_custom(
        Box::new(BytesReader::from_vec(value.to_owned())),
        insert_count,
    )
}

pub fn create_op_file_reader_custom(
    read: Box<dyn Read + Send>,
    insert_count: usize,
) -> OperatorData {
    create_op_file_reader(
        FileKind::Custom(Mutex::new(Some(read))),
        insert_count,
    )
}

// this is an escape hatch if the custom Read to be used does not implement
// clone if this is used, attempting to clone this Operator
// (e.g. while cloning the Context / ContextBuilder that it belongs to) will
// *panic*
pub fn create_op_file_reader_custom_not_cloneable(
    read: Box<dyn Read + Send>,
    insert_count: usize,
) -> OperatorData {
    create_op_file_reader(
        FileKind::Custom(Mutex::new(Some(read))),
        insert_count,
    )
}

pub fn setup_op_file_reader(
    chain: &Chain,
    op: &mut OpFileReader,
) -> Result<(), OperatorSetupError> {
    op.line_buffered = match chain.settings.buffering_mode {
        BufferingMode::BlockBuffer => LineBufferedSetting::No,
        BufferingMode::LineBuffer => LineBufferedSetting::Yes,
        BufferingMode::LineBufferStdin => match op.file_kind {
            FileKind::Stdin => LineBufferedSetting::Yes,
            _ => LineBufferedSetting::No,
        },
        BufferingMode::LineBufferIfTTY => LineBufferedSetting::IfTTY,
        BufferingMode::LineBufferStdinIfTTY => match op.file_kind {
            FileKind::Stdin => LineBufferedSetting::IfTTY,
            _ => LineBufferedSetting::No,
        },
    };
    Ok(())
}

#[derive(Clone)]
pub struct BytesReader {
    pub data: Box<[u8]>,
    pub pos: usize,
}

impl BytesReader {
    pub fn from_string(data: String) -> Self {
        Self {
            data: data.into_boxed_str().into_boxed_bytes(),
            pos: 0,
        }
    }
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self {
            data: data.into_boxed_slice(),
            pos: 0,
        }
    }
}

impl Read for BytesReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.data.len() - self.pos;
        if buf.len() >= len {
            buf[0..len].copy_from_slice(&self.data[self.pos..]);
            self.pos = self.data.len();
            return Ok(len);
        }
        buf.copy_from_slice(&self.data[self.pos..(self.pos + buf.len())]);
        self.pos += buf.len();
        Ok(buf.len())
    }
}
