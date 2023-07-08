use std::{
    ffi::OsStr,
    fs::File,
    io::{BufRead, BufReader, Read, StdinLock},
    path::PathBuf,
    sync::Mutex,
};

use bstr::BStr;
use html5ever::tendril::fmt::Slice;
use smallstr::SmallString;

use crate::{
    field_data::{
        field_value_flags, push_interface::PushInterface, FieldValueFormat, FieldValueHeader,
        FieldValueKind, FieldValueSize, INLINE_STR_MAX_LEN,
    },
    options::argument::CliArgIdx,
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    worker_thread_session::{FieldId, JobData},
};

use super::{
    errors::{io_error_to_op_error, OperatorCreationError},
    operator::{OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

pub enum FileKind {
    Stdin,
    File(PathBuf),
    Custom(Mutex<Option<Box<dyn Read + Send>>>),
}

pub enum AnyFile {
    Stdin(StdinLock<'static>),
    File(File),
    BufferedFile(BufReader<File>),
    Custom(Box<dyn Read>),
    //option so we can take it and raise it as an error later
    FileOpenIoError(Option<std::io::Error>),
}

pub struct OpFileReader {
    file_kind: FileKind,
    line_buffered: bool,
}

impl OpFileReader {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self.file_kind {
            FileKind::Stdin => SmallString::from("stdin"),
            FileKind::File(_) => SmallString::from("file"),
            FileKind::Custom(_) => SmallString::from("__custom_file_stream__"),
        }
    }
}

pub struct TfFileReader {
    // in case of errors, we close this by take()ing the file, therefore option
    file: Option<AnyFile>,
    stream_value: Option<StreamValueId>,
    line_buffered: bool,
    stream_buffer_size: usize,
}

pub fn setup_tf_file_reader<'a>(
    sess: &mut JobData,
    op: &'a OpFileReader,
    tf_state: &mut TransformState,
) -> (TransformData<'a>, FieldId) {
    //TODO: properly set up line buffering
    let file = match &op.file_kind {
        FileKind::Stdin => AnyFile::Stdin(std::io::stdin().lock()),
        FileKind::File(path) => match File::open(path) {
            Ok(f) => {
                if op.line_buffered {
                    AnyFile::File(f)
                } else {
                    AnyFile::BufferedFile(BufReader::new(f))
                }
            }
            Err(e) => AnyFile::FileOpenIoError(Some(e)),
        },
        FileKind::Custom(reader) => AnyFile::Custom(
            reader
                .lock()
                .unwrap()
                .take()
                .expect("attempted to create two transforms from a single custom FileKind"),
        ),
    };

    let data = TransformData::FileReader(TfFileReader {
        file: Some(file),
        stream_value: None,
        line_buffered: op.line_buffered,
        stream_buffer_size: sess.session_data.chains
            [sess.session_data.operator_bases[tf_state.op_id as usize].chain_id as usize]
            .settings
            .stream_buffer_size,
    });
    (data, tf_state.input_field)
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

fn start_streaming_file(sess: &mut JobData<'_>, tf_id: TransformId, fr: &mut TfFileReader) {
    let mut out_field =
        sess.record_mgr.fields[sess.tf_mgr.transforms[tf_id].input_field].borrow_mut();
    // we want to write the chunk straight into field data to avoid a copy
    // SAFETY: this relies on the memory layout in field_data.
    // since that is a submodule of us, this is fine.
    // ideally though, FieldData would expose some way to do this safely.
    let fdi = unsafe { out_field.field_data.internals().fd.internals() };

    let size_before = fdi.data.len();
    let res = read_chunk(
        fdi.data,
        fr.file.as_mut().unwrap(),
        INLINE_STR_MAX_LEN.min(fr.stream_buffer_size),
        fr.line_buffered,
    );
    let chunk_size = match res {
        Ok((size, eof)) => {
            if eof {
                fdi.header.push(FieldValueHeader {
                    fmt: FieldValueFormat {
                        kind: FieldValueKind::BytesInline,
                        flags: field_value_flags::DEFAULT,
                        size: size as FieldValueSize,
                    },
                    run_length: 1,
                });
                *fdi.field_count += 1;
                fr.file.take();
                sess.tf_mgr.unlink_transform(tf_id, 1);
                return;
            }
            size
        }
        Err(err) => {
            let err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id, err);
            out_field.field_data.push_error(err, 1, false, false);
            fr.file.take();
            sess.tf_mgr.unlink_transform(tf_id, 1);
            return;
        }
    };

    let mut buf = Vec::with_capacity(chunk_size);
    buf.extend_from_slice(&fdi.data[size_before..size_before + chunk_size]);
    fdi.data.resize(size_before, 0);
    let sv_id = sess.sv_mgr.stream_values.claim_with_value(StreamValue {
        data: StreamValueData::BytesChunk(buf),
        done: false,
        ref_count: 1,
        bytes_are_utf8: false,
        subscribers: Default::default(),
    });
    fr.stream_value = Some(sv_id);
    out_field
        .field_data
        .push_stream_value_id(sv_id, 1, true, false);
    sess.tf_mgr.make_stream_producer(tf_id);
    sess.tf_mgr.push_tf_in_ready_queue(tf_id);
    sess.tf_mgr.inform_successor_batch_available(tf_id, 1);
}

pub fn handle_tf_file_reader(sess: &mut JobData<'_>, tf_id: TransformId, fr: &mut TfFileReader) {
    let sv_id = if let Some(sv_id) = fr.stream_value {
        sv_id
    } else {
        start_streaming_file(sess, tf_id, fr);
        return;
    };

    let sv = &mut sess.sv_mgr.stream_values[sv_id];
    let res = match &mut sv.data {
        StreamValueData::BytesChunk(ref mut bc) => {
            bc.clear();
            read_chunk(
                bc,
                fr.file.as_mut().unwrap(),
                fr.stream_buffer_size,
                fr.line_buffered,
            )
        }
        StreamValueData::BytesBuffer(ref mut bb) => read_chunk(
            bb,
            fr.file.as_mut().unwrap(),
            fr.stream_buffer_size,
            fr.line_buffered,
        ),
        StreamValueData::Error(_) => {
            fr.file.take();
            Ok((0, true))
        }
        StreamValueData::Dropped => panic!("dropped stream value ovserved"),
    };
    match res {
        Ok((_size, eof)) => {
            if !eof {
                sess.tf_mgr.make_stream_producer(tf_id);
                sess.tf_mgr.push_tf_in_ready_queue(tf_id);
                sess.sv_mgr.inform_stream_value_subscribers(sv_id, false);
                return;
            }
            sv.done = true;
        }
        Err(err) => {
            let err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id, err);
            sv.data = StreamValueData::Error(err);
            sv.done = true;
        }
    }
    sess.sv_mgr.inform_stream_value_subscribers(sv_id, true);
    sess.sv_mgr.drop_field_value_subscription(sv_id, None);
    sess.tf_mgr.unlink_transform(tf_id, 0);
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
        line_buffered: false, //this will be set based on the chain setting during setup
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
        line_buffered: false, //this will be set based on the chain setting during setup
    }))
}

pub fn create_op_file_reader_custom(read: Box<dyn Read + Send>) -> OperatorData {
    OperatorData::FileReader(OpFileReader {
        file_kind: FileKind::Custom(Mutex::new(Some(read))),
        line_buffered: false,
    })
}
