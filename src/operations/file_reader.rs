use std::{
    ffi::OsStr,
    fs::File,
    io::{BufRead, BufReader, Read, StdinLock},
    path::PathBuf,
};

use bstr::BStr;
use html5ever::tendril::fmt::Slice;
use smallstr::SmallString;

use crate::{
    field_data::{
        fd_push_interface::FDPushInterface, field_value_flags, FieldValueFormat, FieldValueHeader,
        FieldValueKind, FieldValueSize, INLINE_STR_MAX_LEN,
    },
    options::argument::CliArgIdx,
    stream_field_data::{StreamFieldValue, StreamFieldValueData, StreamValueId},
    worker_thread_session::{EnterStreamModeFlag, FieldId, JobData, StreamProducerDoneFlag},
};

use super::{
    errors::{io_error_to_op_error, OperatorCreationError},
    operator::{OperatorData, DEFAULT_OP_NAME_SMALL_STR_LEN},
    transform::{TransformData, TransformId, TransformState},
};

pub enum FileKind {
    Stdin,
    File(PathBuf),
}

impl FileKind {
    pub fn default_op_name(&self) -> SmallString<[u8; DEFAULT_OP_NAME_SMALL_STR_LEN]> {
        match self {
            super::file_reader::FileKind::Stdin => SmallString::from("stdin"),
            super::file_reader::FileKind::File(_) => SmallString::from("file"),
        }
    }
}

pub enum AnyFile {
    Stdin(StdinLock<'static>),
    File(File),
    BufferedFile(BufReader<File>),
    //option so we can take it and raise it as an error later
    FileOpenIoError(Option<std::io::Error>),
}

pub struct OpFileReader {
    pub file_kind: FileKind,
    pub line_buffered: bool,
}

pub struct TfFileReader {
    // in case of errors, we close this by take()ing the file, therefore option
    file: Option<AnyFile>,
    stream_value: StreamValueId,
    line_buffered: bool,
}

pub fn setup_tf_file_reader<'a>(
    _sess: &mut JobData,
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
    };
    let data = TransformData::FileReader(TfFileReader {
        file: Some(file),
        stream_value: 0,
        line_buffered: op.line_buffered,
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
    }?;
    Ok((size, eof))
}

fn start_streaming_file(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> EnterStreamModeFlag {
    let (mut batch_size, input_field) = sess.claim_batch(tf_id, &[]);
    let mut out_field = sess.entry_data.fields[input_field].borrow_mut();
    batch_size += 1;
    // we want to write the chunk straight into field data to avoid a copy
    // SAFETY: this relies on the memory layout in field_data.
    // since that is a submodule of us, this is fine.
    // ideally though, FieldData would expose some way to do this safely.
    let ((field_headers, field_data), field_count) = unsafe {
        let internals = out_field.field_data.internals();
        (internals.fd.internals(), internals.field_count)
    };

    let size_before = field_data.len();
    let res = read_chunk(
        field_data,
        fr.file.as_mut().unwrap(),
        INLINE_STR_MAX_LEN,
        fr.line_buffered,
    );
    let chunk_size = match res {
        Ok((size, eof)) => {
            if eof {
                field_headers.push(FieldValueHeader {
                    fmt: FieldValueFormat {
                        kind: FieldValueKind::BytesInline,
                        flags: field_value_flags::DEFAULT,
                        size: size as FieldValueSize,
                    },
                    run_length: 1,
                });
                sess.tf_mgr
                    .inform_successor_batch_available(tf_id, batch_size);
                fr.file.take();
                *field_count += 1;
                return false;
            }
            size
        }
        Err(err) => {
            let err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id, err);
            out_field.field_data.push_error(err, 1, false, false);
            fr.file.take();
            sess.tf_mgr
                .inform_successor_batch_available(tf_id, batch_size);
            return false;
        }
    };

    let mut buf = Vec::with_capacity(chunk_size);
    buf.extend_from_slice(&field_data[size_before..size_before + chunk_size]);
    field_data.resize(size_before, 0);
    fr.stream_value = out_field.stream_field_data.push_value(StreamFieldValue {
        data: StreamFieldValueData::BytesChunk(buf),
        done: false,
        bytes_are_utf8: false,
    });
    out_field
        .field_data
        .push_stream_value_id(fr.stream_value, 1, false, false);
    sess.tf_mgr.push_successor_in_ready_queue(tf_id);
    return true;
}

pub fn handle_tf_file_reader_batch_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> EnterStreamModeFlag {
    if fr.file.is_none() {
        let (batch, field) = sess.claim_batch(tf_id, &[]);
        sess.entry_data.fields[field]
            .borrow_mut()
            .field_data
            .push_unset(batch, true);
        sess.tf_mgr.inform_successor_batch_available(tf_id, batch);
        return false;
    }
    let enter_stream_mode = start_streaming_file(sess, tf_id, fr);
    if enter_stream_mode {
        sess.tf_mgr.stream_producers.push_back(tf_id);
    }
    enter_stream_mode
}

pub fn handle_tf_file_reader_stream_mode(
    _sess: &mut JobData<'_>,
    _tf_id: TransformId,
    _fr: &mut TfFileReader,
) {
    //TODO: ???
}

pub fn handle_tf_file_reader_producer_mode(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> StreamProducerDoneFlag {
    let mut update = true;
    {
        let input_field = sess.tf_mgr.transforms[tf_id].input_field;
        let mut input = sess.entry_data.fields[input_field].borrow_mut();
        let sfd = &mut input.stream_field_data;
        {
            let mut sv = sfd.get_value_mut(fr.stream_value);
            if sv.done {
                sv.data = StreamFieldValueData::Dropped;
                return true;
            }
            let res = match &mut sv.data {
                StreamFieldValueData::BytesChunk(ref mut bc) => {
                    bc.clear();
                    read_chunk(
                        bc,
                        fr.file.as_mut().unwrap(),
                        INLINE_STR_MAX_LEN,
                        fr.line_buffered,
                    )
                }
                StreamFieldValueData::BytesBuffer(ref mut bb) => {
                    update = false;
                    read_chunk(
                        bb,
                        fr.file.as_mut().unwrap(),
                        INLINE_STR_MAX_LEN,
                        fr.line_buffered,
                    )
                }
                StreamFieldValueData::Error(_) => {
                    fr.file.take();
                    Ok((0, true))
                }
                StreamFieldValueData::Dropped => panic!("dropped stream value ovserved"),
            };
            match res {
                Ok((_size, eof)) => {
                    if eof {
                        sv.done = true;
                    }
                }
                Err(err) => {
                    let err = io_error_to_op_error(sess.tf_mgr.transforms[tf_id].op_id, err);
                    sv.data = StreamFieldValueData::Error(err);
                    sv.done = true;
                }
            }
        };
        if update {
            sfd.updates.push(fr.stream_value);
        }
    }
    if update {
        sess.tf_mgr.push_successor_in_ready_queue(tf_id);
    }
    false
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
