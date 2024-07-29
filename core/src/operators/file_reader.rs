use std::{
    fs::File,
    io::{stdin, BufRead, BufReader, ErrorKind, IsTerminal, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    chain::ChainId,
    cli::{
        call_expr::{Argument, CallExpr, Span},
        CliArgumentError,
    },
    job::JobData,
    options::{
        chain_settings::{
            BufferingMode, SettingBufferingMode, SettingStreamBufferSize,
            SettingStreamSizeThreshold,
        },
        session_setup::SessionSetupData,
    },
    record_data::{
        field_data::INLINE_STR_MAX_LEN,
        field_value::ObjectKeysStored,
        iter_hall::IterId,
        push_interface::PushInterface,
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataType, StreamValueId,
        },
    },
    scr_error::ScrError,
};

use super::{
    errors::{io_error_to_op_error, OperatorCreationError},
    multi_op::create_multi_op,
    operator::{
        OperatorBase, OperatorData, OperatorDataId, OperatorId, OperatorName,
        OperatorOffsetInChain,
    },
    regex::{create_op_regex_lines, create_op_regex_trim_trailing_newline},
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
    pub fn default_op_name(&self) -> OperatorName {
        match self.file_kind {
            ReadableFileKind::Stdin => "stdin",
            ReadableFileKind::File(_) => "file",
            ReadableFileKind::Custom(_) => "<custom_file_stream>",
        }
        .into()
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
    let stream_size_threshold =
        jd.get_setting_from_tf_state::<SettingStreamSizeThreshold>(tf_state);
    let stream_buffer_size =
        jd.get_setting_from_tf_state::<SettingStreamBufferSize>(tf_state);

    TransformData::FileReader(TfFileReader {
        file: Some(file),
        stream_value: None,
        line_buffered,
        stream_size_threshold,
        stream_buffer_size,
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
    let mut bis = output_field.iter_hall.bytes_insertion_stream(1);

    // we don't want to initially block on stdin in case it isn't ready
    let skip_initial_read = matches!(fr.file, Some(AnyFileReader::Stdin));
    let res = if skip_initial_read {
        Ok((0, false))
    } else {
        read_chunk(
            &mut bis,
            fr.file.as_mut().unwrap(),
            INLINE_STR_MAX_LEN
                .min(fr.stream_buffer_size)
                .min(fr.stream_size_threshold),
            fr.line_buffered,
        )
    };

    match res {
        Ok((_size, eof)) => {
            if eof {
                fr.file.take();
                bis.commit();
                return;
            }
        }
        Err(e) => {
            bis.abort();
            let err = io_error_to_op_error(
                jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                &e,
            );
            output_field.iter_hall.push_error(err, 1, false, false);
            fr.file.take();
            return;
        }
    }
    let mut buf = Vec::with_capacity(fr.stream_buffer_size);
    buf.extend(bis.get_inserted_data());
    bis.abort();
    let buf_len = buf.len();
    if buf_len < fr.stream_buffer_size && !skip_initial_read {
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

pub fn handle_tf_file_reader_stream_producer_update(
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

    let mut drop_unused = true;

    let tf = &jd.tf_mgr.transforms[tf_id];
    if !tf.predecessor_done {
        drop_unused = false;
        // if we aren't done, subsequent records might need the stream
        sv.buffer_mode.require_buffered();
    }

    if sv.ref_count > 1 {
        drop_unused = false;
    }
    let mut done = drop_unused;

    if !drop_unused {
        let res = sv
            .data_inserter(sv_id, fr.stream_buffer_size, true)
            .with_bytes_buffer(|buf| {
                read_chunk(buf, file, fr.stream_buffer_size, fr.line_buffered)
            });

        done = match res {
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
    }

    jd.sv_mgr.inform_stream_value_subscribers(sv_id);
    if done {
        fr.file.take();
        // we only drop our stream value once all input is already handled
        if jd.tf_mgr.transforms[tf_id].done {
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

#[derive(Default, Clone, Copy)]
struct FileReaderOptions {
    lines: bool,
    insert_count: Option<usize>,
}

fn parse_file_reader_flags(
    expr: &CallExpr,
    flags: Option<&ObjectKeysStored>,
) -> Result<FileReaderOptions, CliArgumentError> {
    let mut opts = FileReaderOptions::default();
    let Some(flags) = flags else {
        return Ok(opts);
    };
    for (k, v) in flags {
        let arg = v.downcast_ref::<Argument>().unwrap();
        if k == "l" || k == "lines" {
            expr.expect_flag(k, v)?;
            opts.lines = true;
            continue;
        }
        if k == "n" || k == "count" {
            let Some(value) = arg.value.try_cast_int(true) else {
                return Err(expr.error_arg_invalid_int(k, arg.span));
            };
            opts.insert_count = Some(value as usize);
            continue;
        }
        return Err(expr.error_flag_unsupported(k, arg.span));
    }
    Ok(opts)
}

pub fn parse_op_file_reader(
    sess: &SessionSetupData,
    mut expr: CallExpr,
) -> Result<OperatorData, ScrError> {
    expr.split_flags_arg_normalized(&sess.string_store, true);
    let (flags, args) = expr.split_flags_arg(true);

    let opts = parse_file_reader_flags(&expr, flags)?;

    if args.len() != 1 {
        return Err(expr.error_require_exact_positional_count(1).into());
    }
    let value = args[0].expect_plain(expr.op_name)?;
    build_op_file(value, opts, expr.span)
}

pub fn parse_op_stdin(
    sess: &SessionSetupData,
    mut expr: CallExpr,
) -> Result<OperatorData, OperatorCreationError> {
    expr.split_flags_arg_normalized(&sess.string_store, true);
    let (flags, args) = expr.split_flags_arg(true);
    if !args.is_empty() {
        return Err(expr
            .error_positional_args_unsupported(args[0].span)
            .into());
    }
    let opts = parse_file_reader_flags(&expr, flags)?;
    Ok(build_op_stdin(opts))
}

#[allow(clippy::unnecessary_wraps)]
fn build_op_file(
    value: &[u8],
    opts: FileReaderOptions,
    #[cfg_attr(unix, allow(unused))] span: Span,
) -> Result<OperatorData, ScrError> {
    let path = {
        #[cfg(unix)]
        {
            use std::ffi::OsStr;
            PathBuf::from(
                <OsStr as std::os::unix::prelude::OsStrExt>::from_bytes(value),
            )
        }
        #[cfg(windows)]
        {
            use bstr::ByteSlice;
            PathBuf::from(value.to_str().map_err(|_| {
                OperatorCreationError::new(
                    "failed to parse file path argument as unicode",
                    span,
                )
            })?)
        }
    };
    let op = create_op_file(path, opts.insert_count.unwrap_or(0));
    if opts.lines {
        // TODO: create optimized version of this
        return Ok(create_multi_op([
            op,
            create_op_regex_trim_trailing_newline(),
            create_op_regex_lines(),
        ]));
    }
    Ok(op)
}

fn build_op_stdin(opts: FileReaderOptions) -> OperatorData {
    let op = create_op_stdin(opts.insert_count.unwrap_or(0));
    if opts.lines {
        // TODO: create optimized version of this
        return create_multi_op([
            op,
            create_op_regex_trim_trailing_newline(),
            create_op_regex_lines(),
        ]);
    }
    op
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
    op: &mut OpFileReader,
    sess: &mut SessionSetupData,
    op_data_id: OperatorDataId,
    chain_id: ChainId,
    offset_in_chain: OperatorOffsetInChain,
    span: Span,
) -> Result<OperatorId, ScrError> {
    let buffering_mode =
        sess.get_chain_setting::<SettingBufferingMode>(chain_id);
    op.line_buffered = match buffering_mode {
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
    Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
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
