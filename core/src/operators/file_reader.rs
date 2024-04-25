use std::{
    ffi::OsStr,
    fs::File,
    io::{stdin, BufRead, BufReader, ErrorKind, IsTerminal, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use regex::Regex;
use smallstr::SmallString;

use crate::{
    chain::{BufferingMode, Chain},
    job::JobData,
    options::argument::CliArgIdx,
    record_data::{
        field_data::{
            field_value_flags, FieldValueFormat, FieldValueRepr,
            FieldValueSize, INLINE_STR_MAX_LEN,
        },
        iter_hall::IterId,
        push_interface::PushInterface,
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataType, StreamValueId,
        },
    },
};

use super::{
    errors::{
        io_error_to_op_error, OperatorCreationError, OperatorSetupError,
    },
    operator::{DefaultOperatorName, OperatorBase, OperatorData},
    transform::{TransformData, TransformId, TransformState},
    utils::maintain_single_value::{maintain_single_value, ExplicitCount},
};

pub enum ReadableFileKind {
    Stdin,
    File(PathBuf),
    Custom(Mutex<Option<Box<dyn Read + Send>>>),
}

pub enum AnyFileReader {
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
    file_kind: ReadableFileKind,
    line_buffered: LineBufferedSetting,
    insert_count: Option<usize>,
}

impl OpFileReader {
    pub fn default_op_name(&self) -> DefaultOperatorName {
        let mut res = SmallString::new();
        match self.file_kind {
            ReadableFileKind::Stdin => res.push_str("stdin"),
            ReadableFileKind::File(_) => res.push_str("file"),
            ReadableFileKind::Custom(_) => {
                res.push_str("<custom_file_stream>")
            }
        }
        res
    }
}

pub struct TfFileReader {
    // in case of errors, we close this by take()ing the file, therefore
    // option
    file: Option<AnyFileReader>,
    stream_value: Option<StreamValueId>,
    value_committed: bool,
    line_buffered: bool,
    stream_buffer_size: usize,
    stream_size_threshold: usize,
    explicit_count: Option<ExplicitCount>,
    iter_id: IterId,
}

pub fn build_tf_file_reader<'a>(
    jd: &mut JobData,
    _op_base: &OperatorBase,
    op: &'a OpFileReader,
    tf_state: &TransformState,
) -> TransformData<'a> {
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
        ReadableFileKind::Stdin => {
            let stdin = std::io::stdin().lock();
            if check_if_tty {
                line_buffered = stdin.is_terminal();
            }
            AnyFileReader::Stdin
        }
        ReadableFileKind::File(path) => match File::open(path) {
            Ok(f) => {
                if check_if_tty && f.is_terminal() {
                    line_buffered = true;
                    AnyFileReader::BufferedFile(BufReader::new(f))
                } else {
                    AnyFileReader::File(f)
                }
            }
            Err(e) => AnyFileReader::FileOpenIoError(Some(e)),
        },
        ReadableFileKind::Custom(reader) => {
            AnyFileReader::Custom(reader
                .lock()
                .unwrap()
                .take()
                .expect("attempted to create two transforms from a single custom FileKind"))
        }
    };
    let chain_settings =
        &jd.get_transform_chain_from_tf_state(tf_state).settings;
    TransformData::FileReader(TfFileReader {
        file: Some(file),
        stream_value: None,
        line_buffered,
        stream_size_threshold: chain_settings.stream_size_threshold,
        stream_buffer_size: chain_settings.stream_buffer_size,
        explicit_count: op.insert_count.map(|count| ExplicitCount {
            count,
            actor_id: jd.add_actor_for_tf_state(tf_state),
        }),
        iter_id: jd.add_iter_for_tf_state(tf_state),
        value_committed: false,
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
            if let Some(i) = memchr::memchr(b'\n', available) {
                target.write_all(&available[..=i])?;
                (true, i + 1)
            } else {
                target.write_all(available)?;
                (false, available.len())
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
    file: &mut AnyFileReader,
    limit: usize,
    line_buffered: bool,
) -> Result<(usize, bool), std::io::Error> {
    let (size, eof) = match file {
        AnyFileReader::BufferedFile(f) => {
            read_mode_based(f, limit, target, line_buffered)
        }
        AnyFileReader::Stdin => {
            read_mode_based(&mut stdin().lock(), limit, target, line_buffered)
        }
        AnyFileReader::File(f) => read_size_limited(f, limit, target),
        AnyFileReader::FileOpenIoError(e) => Err(e.take().unwrap()),
        AnyFileReader::Custom(r) => read_size_limited(r, limit, target),
    }?;
    Ok((size, eof))
}

fn start_streaming_file(
    jd: &mut JobData,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) {
    let out_fid = jd.tf_mgr.prepare_output_field(
        &mut jd.field_mgr,
        &mut jd.match_set_mgr,
        tf_id,
    );

    let mut output_field = jd.field_mgr.fields[out_fid].borrow_mut();
    // we want to write the chunk straight into field data to avoid a copy
    // SAFETY: this relies on the memory layout in field_data.
    // since that is a submodule of us, this is fine.
    // ideally though, FieldData would expose some way to do this safely.
    let fdi = unsafe { output_field.iter_hall.internals_mut() };

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
                jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                &e,
            );
            output_field.iter_hall.push_error(err, 1, false, false);
            fr.file.take();
            return;
        }
    };
    let mut buf = Vec::with_capacity(fr.stream_buffer_size);
    buf.extend(fdi.data.range(size_before..(size_before + chunk_size)));
    fdi.data.truncate(size_before);
    let buf_len = buf.len();
    if buf_len < fr.stream_buffer_size {
        match read_chunk(
            &mut buf,
            fr.file.as_mut().unwrap(),
            fr.stream_buffer_size - buf_len,
            fr.line_buffered,
        ) {
            Ok((_size, eof)) => {
                if eof {
                    fr.file.take();
                    output_field
                        .iter_hall
                        .push_bytes_buffer(buf, 1, false, false);
                    return;
                }
            }
            Err(e) => {
                let err = io_error_to_op_error(
                    jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                    &e,
                );
                output_field.iter_hall.push_error(err, 1, false, false);
                fr.file.take();
                return;
            }
        }
    };
    let sv_id = jd.sv_mgr.claim_stream_value(StreamValue::from_data(
        Some(StreamValueDataType::Bytes),
        StreamValueData::Bytes {
            range: 0..buf.len(),
            data: Arc::new(buf),
        },
        StreamValueBufferMode::Stream,
        false,
    ));
    fr.stream_value = Some(sv_id);
    output_field
        .iter_hall
        .push_stream_value_id(sv_id, 1, false, false);
    drop(output_field);
    // even if the stream is already done, we can only drop the stream value
    // next time we are called once it was observed -> refcounted
    jd.tf_mgr.make_stream_producer(tf_id);
}

pub fn handle_tf_file_reader_stream(
    jd: &mut JobData,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) {
    let sv_id = fr.stream_value.unwrap();
    let Some(file) = fr.file.as_mut() else {
        // this happens if the stream is immediately done,
        // but we had to wait for subscribers before unregistering it
        jd.sv_mgr.drop_field_value_subscription(sv_id, None);
        return;
    };
    let sv = &mut jd.sv_mgr.stream_values[sv_id];
    let tf = &jd.tf_mgr.transforms[tf_id];
    if !tf.predecessor_done {
        // if we aren't done, subsequent records might need the stream
        sv.buffer_mode.require_buffered();
    }

    let res = sv
        .data_inserter(sv_id, fr.stream_buffer_size, true)
        .with_bytes_buffer(|buf| {
            read_chunk(buf, file, fr.stream_buffer_size, fr.line_buffered)
        });

    let done = match res {
        Ok((_size, eof)) => {
            sv.done = eof;
            eof
        }
        Err(err) => {
            let err = io_error_to_op_error(
                jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                &err,
            );
            sv.set_error(Arc::new(err));
            true
        }
    };

    jd.sv_mgr.inform_stream_value_subscribers(sv_id);
    if done {
        fr.file.take();
        // we only drop our stream value once all input is already handled
        if jd.tf_mgr.transforms[tf_id].mark_for_removal {
            jd.sv_mgr.drop_field_value_subscription(sv_id, None);
        }
        return;
    }
    jd.tf_mgr.make_stream_producer(tf_id);
}

pub fn handle_tf_file_reader(
    jd: &mut JobData,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) {
    if !fr.value_committed {
        fr.value_committed = true;
        start_streaming_file(jd, tf_id, fr);
    }
    let (batch_size, ps) = maintain_single_value(
        jd,
        tf_id,
        fr.explicit_count.as_ref(),
        fr.iter_id,
    );
    jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
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
                    "failed to parse insertion count as an integer",
                    arg_idx,
                )
            })
        })
        .transpose()?;
    match args.name("kind").unwrap().as_str() {
        "file" => parse_op_file(value, insert_count, arg_idx),
        "stdin" | "in" => parse_op_stdin(value, insert_count, arg_idx),
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

// insert count 0 means all input will be consumed
pub fn create_op_file_reader(
    file_kind: ReadableFileKind,
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
    create_op_file_reader(ReadableFileKind::File(path), insert_count)
}
pub fn create_op_stdin(insert_count: usize) -> OperatorData {
    create_op_file_reader(ReadableFileKind::Stdin, insert_count)
}

pub fn create_op_stream_dummy(
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
        ReadableFileKind::Custom(Mutex::new(Some(read))),
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
        ReadableFileKind::Custom(Mutex::new(Some(read))),
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
            ReadableFileKind::Stdin => LineBufferedSetting::Yes,
            _ => LineBufferedSetting::No,
        },
        BufferingMode::LineBufferIfTTY => LineBufferedSetting::IfTTY,
        BufferingMode::LineBufferStdinIfTTY => match op.file_kind {
            ReadableFileKind::Stdin => LineBufferedSetting::IfTTY,
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
