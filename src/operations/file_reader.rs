use std::{
    fs::File,
    io::{BufRead, BufReader, Read, StdinLock},
};

use crate::{
    field_data::{
        fd_push_interface::FDPushInterface, field_value_flags, FieldValueFormat, FieldValueHeader,
        FieldValueKind, FieldValueSize, INLINE_STR_MAX_LEN,
    },
    stream_field_data::{StreamFieldValue, StreamFieldValueData, StreamValueId},
    worker_thread_session::{
        EnterStreamModeFlag, FieldId, JobData, MatchSetId, StreamProducerDoneFlag, TransformManager,
    },
};

use super::{
    errors::io_error_to_op_error,
    operator::OperatorId,
    transform::{TransformData, TransformId, TransformState},
};

pub enum FileType<'a> {
    Stdin(StdinLock<'a>),
    File(File),
    BufferedFile(BufReader<File>),
}

pub struct TfFileReader<'a> {
    file: Option<FileType<'a>>,
    output_field: FieldId,
    stream_value: StreamValueId,
    line_buffered: bool,
}

//TODO: add this as an operator aswell, only used for stdin/file for now
pub fn setup_tf_file_reader_as_entry_point<'a>(
    tf_mgr: &mut TransformManager,
    input_field: FieldId,
    output_field: FieldId,
    ms_id: MatchSetId,
    desired_batch_size: usize,
    mut file: FileType<'a>,
    line_buffered: bool,
) -> (TransformState, TransformData<'a>) {
    let state = TransformState {
        input_field,
        available_batch_size: 1,
        match_set_id: ms_id,
        successor: None,
        predecessor: None,
        desired_batch_size,
        op_id: OperatorId::MAX,
        ordering_id: tf_mgr.claim_transform_ordering_id(),
        last_consumed_batch_size: 0,
        is_ready: false,
        is_stream_producer: false,
    };
    if line_buffered {
        if let FileType::File(f) = file {
            file = FileType::BufferedFile(BufReader::new(f));
        }
    }
    let data = TransformData::FileReader(TfFileReader {
        file: Some(file),
        output_field: output_field,
        stream_value: 0,
        line_buffered,
    });
    (state, data)
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
    file: &mut FileType,
    limit: usize,
    line_buffered: bool,
) -> Result<(usize, bool), std::io::Error> {
    let (size, eof) = match file {
        FileType::BufferedFile(f) => read_mode_based(f, limit, target, line_buffered),
        FileType::Stdin(f) => read_mode_based(f, limit, target, line_buffered),
        FileType::File(f) => read_size_limited(f, limit, target),
    }?;
    Ok((size, eof))
}

fn start_streaming_file(
    sess: &mut JobData<'_>,
    tf_id: TransformId,
    fr: &mut TfFileReader,
) -> EnterStreamModeFlag {
    let (batch_size, _) = sess.claim_batch(tf_id, &[fr.output_field]);
    debug_assert!(batch_size == 1);
    let mut out_field = sess.entry_data.fields[fr.output_field].borrow_mut();

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
                sess.tf_mgr.inform_successor_batch_available(tf_id, 1);
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
            sess.tf_mgr.inform_successor_batch_available(tf_id, 1);
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
        let (batch, _) = sess.claim_batch(tf_id, &[fr.output_field]);
        sess.entry_data.fields[fr.output_field]
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
        let mut input = sess.entry_data.fields[fr.output_field].borrow_mut();
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
