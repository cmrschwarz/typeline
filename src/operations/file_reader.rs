use std::{
    ffi::OsStr,
    fs::File,
    io::{BufRead, BufReader, Read, StdinLock},
    path::PathBuf,
    sync::Mutex,
};

use bstr::{BStr, ByteSlice};
use is_terminal::IsTerminal;
use smallstr::SmallString;

use crate::{
    chain::{BufferingMode, Chain},
    field_data::{
        field_value_flags,
        push_interface::{PushInterface, UnsafeHeaderPushInterface},
        FieldValueFormat, FieldValueKind, FieldValueSize, INLINE_STR_MAX_LEN,
    },
    options::argument::CliArgIdx,
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    worker_thread_session::JobSession,
};

use super::{
    errors::{io_error_to_op_error, OperatorCreationError, OperatorSetupError},
    operator::{OperatorBase, OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

pub trait ReadSendCloneDyn: Read + Send {
    fn clone_dyn(&self) -> Box<dyn ReadSendCloneDyn>;
    fn clone_as_read(&self) -> Box<dyn Read>;
}
impl<T: Read + Send + Clone + 'static> ReadSendCloneDyn for T {
    fn clone_dyn(&self) -> Box<dyn ReadSendCloneDyn> {
        Box::new(self.clone())
    }
    fn clone_as_read(&self) -> Box<dyn Read> {
        Box::new(self.clone())
    }
}

pub enum FileKind {
    Stdin,
    File(PathBuf),
    Custom(Mutex<Option<Box<dyn ReadSendCloneDyn>>>),
    CustomNotCloneable(Mutex<Option<Box<dyn Read + Send>>>),
}

impl Clone for FileKind {
    fn clone(&self) -> Self {
        match self {
            Self::Stdin => Self::Stdin,
            Self::File(path) => Self::File(path.clone()),
            Self::CustomNotCloneable(_) => {
                panic!("attempted to clone a OpFileReader containing FileKind::CustomNotCloneable. Consider Using FileKind::Custom instead");
            }
            Self::Custom(cust) => Self::Custom(Mutex::new({
                let guard = cust.lock().unwrap();
                if let Some(v) = &*guard {
                    Some(v.clone_dyn())
                } else {
                    None
                }
            })),
        }
    }
}

pub enum AnyFile {
    Stdin(StdinLock<'static>),
    File(File),
    BufferedFile(BufReader<File>),
    Custom(Box<dyn Read>),
    //option so we can take it and raise it as an error later
    FileOpenIoError(Option<std::io::Error>),
}

#[derive(Clone)]
enum LineBufferedSetting {
    Yes,
    No,
    IfTTY,
}

#[derive(Clone)]
pub struct OpFileReader {
    file_kind: FileKind,
    line_buffered: LineBufferedSetting,
}

impl OpFileReader {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        let mut res = SmallString::new();
        match self.file_kind {
            FileKind::Stdin => res.push_str("stdin"),
            FileKind::File(_) => res.push_str("file"),
            FileKind::Custom(_) => res.push_str("<custom_file_stream>"),
            FileKind::CustomNotCloneable(_) => res.push_str("<custom_file_stream_not_cloneable>"),
        }
        res
    }
}

pub struct TfFileReader {
    // in case of errors, we close this by take()ing the file, therefore option
    file: Option<AnyFile>,
    stream_value: Option<StreamValueId>,
    line_buffered: bool,
    stream_buffer_size: usize,
    dont_claim_input: bool,
}

pub fn setup_tf_file_reader<'a>(
    sess: &mut JobSession,
    _op_base: &OperatorBase,
    op: &'a OpFileReader,
    tf_state: &mut TransformState,
) -> TransformData<'a> {
    //TODO: properly set up line buffering
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
            AnyFile::Stdin(stdin)
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
        FileKind::CustomNotCloneable(reader) => AnyFile::Custom(
            reader
                .lock()
                .unwrap()
                .take()
                .expect("attempted to create two transforms from a single custom FileKind"),
        ),
        FileKind::Custom(reader) => {
            let thebox = reader
                .lock()
                .unwrap()
                .take()
                .expect("attempted to create two transforms from a single custom FileKind");
            let trait_casted_box: Box<dyn Read>;
            #[cfg(feature = "trait_upcasting")]
            {
                trait_casted_box = thebox;
            }
            #[cfg(not(feature = "trait_upcasting"))]
            {
                // this is sad
                trait_casted_box = thebox.clone_as_read();
            }
            AnyFile::Custom(trait_casted_box)
        }
    };

    TransformData::FileReader(TfFileReader {
        file: Some(file),
        stream_value: None,
        line_buffered,
        stream_buffer_size: sess.session_data.chains
            [sess.session_data.operator_bases[tf_state.op_id.unwrap() as usize].chain_id as usize]
            .settings
            .stream_buffer_size,
        dont_claim_input: false,
    })
}

fn read_size_limited<F: Read>(
    f: &mut F,
    limit: usize,
    target: &mut Vec<u8>,
) -> Result<(usize, bool), std::io::Error> {
    let size = f
        .take(limit as u64)
        .read_to_end(target)
        .map(|size| size as usize)?;
    let eof = size != limit;
    Ok((size, eof))
}
fn read_line_buffered<F: BufRead>(
    f: &mut F,
    limit: usize,
    target: &mut Vec<u8>,
) -> Result<(usize, bool), std::io::Error> {
    let size = f
        .take(limit as u64)
        .read_until('\n' as u8, target)
        .map(|size| size as usize)?;
    let eof = size == 0 || target[size - 1] != '\n' as u8;
    Ok((size, eof))
}
fn read_mode_based<F: BufRead>(
    f: &mut F,
    limit: usize,
    target: &mut Vec<u8>,
    line_buffered: bool,
) -> Result<(usize, bool), std::io::Error> {
    if line_buffered {
        read_line_buffered(f, limit, target)
    } else {
        read_size_limited(f, limit, target)
    }
}

fn read_chunk(
    target: &mut Vec<u8>,
    file: &mut AnyFile,
    limit: usize,
    line_buffered: bool,
) -> Result<(usize, bool), std::io::Error> {
    let (size, eof) = match file {
        AnyFile::BufferedFile(f) => read_mode_based(f, limit, target, line_buffered),
        AnyFile::Stdin(f) => read_mode_based(f, limit, target, line_buffered),
        AnyFile::File(f) => read_size_limited(f, limit, target),
        AnyFile::FileOpenIoError(e) => Err(e.take().unwrap()),
        AnyFile::Custom(r) => read_size_limited(r, limit, target),
    }?;
    Ok((size, eof))
}

fn start_streaming_file(sess: &mut JobSession<'_>, tf_id: TransformId, fr: &mut TfFileReader) {
    let tf = &sess.tf_mgr.transforms[tf_id];
    let has_cont = tf.continuation.is_some();
    let output_field_id = tf.output_field;
    sess.prepare_for_output(tf_id, &[output_field_id]);
    // if there might be more records later we must always buffer the file data

    let (mut batch_size, input_done) = sess.tf_mgr.claim_batch(tf_id);
    if batch_size == 0 {
        debug_assert!(input_done);
        batch_size = 1;
    } else if batch_size > 1 && has_cont {
        sess.tf_mgr.unclaim_batch_size(tf_id, batch_size - 1);
        batch_size = 1;
        fr.dont_claim_input = true;
    }
    let mut out_field = sess.field_mgr.fields[output_field_id].borrow_mut();
    // we want to write the chunk straight into field data to avoid a copy
    // SAFETY: this relies on the memory layout in field_data.
    // since that is a submodule of us, this is fine.
    // ideally though, FieldData would expose some way to do this safely.
    let fdi = unsafe { out_field.field_data.internals() };

    let size_before = fdi.data.len();
    let res = read_chunk(
        fdi.data,
        fr.file.as_mut().unwrap(),
        INLINE_STR_MAX_LEN.min(fr.stream_buffer_size),
        fr.line_buffered,
    );
    let mut done = false;
    let chunk_size = match res {
        Ok((size, eof)) => {
            if eof {
                fr.file.take();
            }
            if !input_done {
                // always buffer in case of additional input for now
                // PERF: we could allow for inline, or even buffered here
                // if we don't want to (depends on values of stream_buffer_size)
                // we should not write into the inline buffer
                done = eof;
            } else if eof {
                *fdi.field_count += batch_size;
                unsafe {
                    out_field.field_data.raw().add_header_for_single_value(
                        FieldValueFormat {
                            kind: FieldValueKind::BytesInline,
                            flags: field_value_flags::SHARED_VALUE,
                            size: size as FieldValueSize,
                        },
                        batch_size,
                        false,
                        false,
                    );
                }
                drop(out_field);
                sess.unlink_transform(tf_id, batch_size);
                return;
            }
            size
        }
        Err(err) => {
            let err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id.unwrap(), err);
            out_field
                .field_data
                .push_error(err, batch_size, false, false);
            fr.file.take();
            drop(out_field);
            sess.unlink_transform(tf_id, batch_size);
            return;
        }
    };

    //TODO: add another read here to reach fr.stream_buffer_size
    let mut buf = Vec::with_capacity(chunk_size);
    buf.extend_from_slice(&fdi.data[size_before..(size_before + chunk_size)]);

    fdi.data.resize(size_before, 0);
    let sv_id = sess.sv_mgr.stream_values.claim_with_value(StreamValue {
        data: StreamValueData::Bytes(buf),
        done,
        ref_count: 1,
        bytes_are_utf8: false,
        bytes_are_chunk: input_done,
        subscribers: Default::default(),
    });
    fr.stream_value = Some(sv_id);
    out_field
        .field_data
        .push_stream_value_id(sv_id, batch_size, false, false);
    sess.tf_mgr
        .inform_successor_batch_available(tf_id, batch_size);
    sess.tf_mgr.make_stream_producer(tf_id);
    sess.tf_mgr.push_tf_in_ready_queue(tf_id);
}

pub fn handle_tf_file_reader(sess: &mut JobSession<'_>, tf_id: TransformId, fr: &mut TfFileReader) {
    let sv_id = if let Some(sv_id) = fr.stream_value {
        sv_id
    } else {
        start_streaming_file(sess, tf_id, fr);
        return;
    };

    let (additional_batch_size, input_done) = if fr.dont_claim_input {
        (0, true)
    } else {
        sess.tf_mgr.claim_batch(tf_id)
    };
    if additional_batch_size > 0 {
        let mut output_field = sess.prepare_output_field(tf_id);
        output_field
            .field_data
            .push_stream_value_id(sv_id, additional_batch_size, true, true);
    }
    let mut file_eof = true;
    if let Some(file) = &mut fr.file {
        let sv = &mut sess.sv_mgr.stream_values[sv_id];
        let res = match &mut sv.data {
            StreamValueData::Bytes(ref mut bc) => {
                if sv.bytes_are_chunk {
                    bc.clear();
                }
                read_chunk(bc, file, fr.stream_buffer_size, fr.line_buffered)
            }
            StreamValueData::Error(_) => Ok((0, true)),
            StreamValueData::Dropped => panic!("dropped stream value ovserved"),
        };
        match res {
            Ok((_size, eof)) => {
                if !eof {
                    file_eof = false;
                } else {
                    fr.file.take();
                    sv.done = true;
                }
            }
            Err(err) => {
                let err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id.unwrap(), err);
                sv.data = StreamValueData::Error(err);
                sv.done = true;
                fr.file.take();
            }
        }
        sess.sv_mgr.inform_stream_value_subscribers(sv_id);
    }
    if !file_eof {
        sess.tf_mgr.make_stream_producer(tf_id);
        sess.tf_mgr.push_tf_in_ready_queue(tf_id);
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, additional_batch_size);
    } else if input_done {
        if additional_batch_size > 0 {
            sess.tf_mgr
                .inform_successor_batch_available(tf_id, additional_batch_size);
            // so we can drop the field value once that last batch of input is done with it
            sess.tf_mgr.push_tf_in_ready_queue(tf_id);
        } else {
            sess.sv_mgr.drop_field_value_subscription(sv_id, None);
            sess.unlink_transform(tf_id, additional_batch_size);
        }
    } else {
        sess.tf_mgr
            .inform_successor_batch_available(tf_id, additional_batch_size);
    }
}

pub fn parse_op_file(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let path = if let Some(value) = value {
        #[cfg(unix)]
        {
            PathBuf::from(<OsStr as std::os::unix::prelude::OsStrExt>::from_bytes(
                value.as_bytes(),
            ))
        }
        #[cfg(windows)]
        {
            PathBuf::from(value.to_str().map_err(|_| {
                OperatorCreationError::new("failed to parse file path argument as unicode", arg_idx)
            })?)
        }
    } else {
        return Err(OperatorCreationError::new(
            "missing path argument for file",
            arg_idx,
        ));
    };

    Ok(OperatorData::FileReader(OpFileReader {
        file_kind: FileKind::File(path),
        line_buffered: LineBufferedSetting::No, //this will be set based on the chain setting during setup
    }))
}

pub fn parse_op_stdin(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    if value.is_some() {
        return Err(OperatorCreationError::new(
            "stdin does not take arguments",
            arg_idx,
        ));
    };

    Ok(OperatorData::FileReader(OpFileReader {
        file_kind: FileKind::Stdin,
        line_buffered: LineBufferedSetting::No, //this will be set based on the chain setting during setup
    }))
}

pub fn create_op_file_reader_custom(read: Box<dyn ReadSendCloneDyn>) -> OperatorData {
    OperatorData::FileReader(OpFileReader {
        file_kind: FileKind::Custom(Mutex::new(Some(read))),
        line_buffered: LineBufferedSetting::No,
    })
}

// this is an escape hatch if the custom Read to be used does not implement clone
// if this is used, attempting to clone this Operator
// (e.g. while cloning the Context / ContextBuilder that it belongs to) will *panic*
pub fn create_op_file_reader_custom_not_cloneable(read: Box<dyn Read + Send>) -> OperatorData {
    OperatorData::FileReader(OpFileReader {
        file_kind: FileKind::CustomNotCloneable(Mutex::new(Some(read))),
        line_buffered: LineBufferedSetting::No,
    })
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
