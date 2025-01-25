use core::str;
use std::{
    cell::{RefCell, RefMut},
    io::{BufRead, Write},
    path::PathBuf,
    sync::Mutex,
};

use memchr::memchr2;
use typeline_core::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
    },
    operators::{
        errors::{OperatorApplicationError, OperatorSetupError},
        operator::{
            OffsetInChain, Operator, OperatorDataId, OperatorId,
            OperatorOffsetInChain, PreboundOutputsMap, TransformInstatiation,
        },
        transform::{Transform, TransformId, TransformState},
        utils::readable::{AnyBufReader, ReadableTarget},
    },
    options::session_setup::SessionSetupData,
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::FieldId,
        field_data::FieldData,
        group_track::GroupTrackIterRef,
        iter_hall::IterKind,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    typeline_error::TypelineError,
    utils::{
        counting_writer::RememberLastCharacterWriter,
        indexing_type::IndexingType,
        int_string_conversions::usize_to_str,
        stable_vec::StableVec,
        string_store::{StringStore, StringStoreEntry},
        temp_vec::TransmutableContainer,
        test_utils::{read_until_2, read_until_3},
    },
};

// HACK
// TODO: proper dynamic field management
const INITIAL_OUTPUT_COUNT: usize = 6;

pub struct OpCsv {
    header: bool,
    disable_quotes: bool,
    var_names: Vec<StringStoreEntry>,
    unused_fields: Vec<bool>,
    input: ReadableTarget,
    reader: Mutex<Option<AnyBufReader>>,
}

pub enum Input {
    NotStarted,
    Running(AnyBufReader),
    Error(OperatorApplicationError),
}

struct PendingField {
    name: String,
    data: RefCell<FieldData>,
}

pub struct TfCsv<'a> {
    op: &'a OpCsv,
    input: Input,
    output_fields: Vec<FieldId>,
    inserters: Vec<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    additional_fields: StableVec<PendingField>,
    lines_produced: usize,
    actor_id: ActorId,
    group_iter: GroupTrackIterRef,
}

struct ReadStatus {
    lines_produced: usize,
    col_idx: usize,
    done: bool,
}

struct CsvReadOptions<'a> {
    unused_fields: &'a [bool],
    disable_quotes: bool,
    prefix_nulls: usize,
    lines_max: usize,
}

enum CsvTerminal {
    Comma,
    Newline,
    Eof,
}

fn parse_quoted(
    reader: &'_ mut impl BufRead,
    out: &mut Vec<u8>,
) -> Result<CsvTerminal, std::io::Error> {
    reader.consume(1);
    loop {
        let read = read_until_3(reader, out, b',', b'\"', b'\n')?;
        if read == 0 {
            // TODO: add strict mode?
            return Ok(CsvTerminal::Eof);
        }
        let mut len = out.len();
        if out[len - 1] == b'"' {
            let buf = reader.fill_buf()?;
            len = buf.len();
            if len == 0 {
                out.truncate(len - 1);
                return Ok(CsvTerminal::Eof);
            }
            if buf[0] == b'"' {
                reader.consume(1);
                continue;
            }
            if buf[0] == b',' {
                out.truncate(len - 1);
                reader.consume(1);
                return Ok(CsvTerminal::Comma);
            }
            continue; // TODO: add strict mode?
        }
        if out[len - 1] == b'\n' {
            if read > 1 && out[len - 2] == b'\r' {
                out.truncate(len - 2);
            } else {
                out.truncate(len - 1);
            }
            return Ok(CsvTerminal::Newline);
        }
        debug_assert_eq!(out[out.len() - 1], b',');
        out.truncate(len - 1);
        return Ok(CsvTerminal::Comma);
    }
}

fn parse_unquoted(
    reader: &'_ mut impl BufRead,
    out: &mut Vec<u8>,
) -> Result<CsvTerminal, std::io::Error> {
    let read = read_until_2(reader, out, b',', b'\n')?;
    if read == 0 {
        return Ok(CsvTerminal::Eof);
    }
    let len = out.len();
    if out[len - 1] == b'\n' {
        if read > 1 && out[len - 2] == b'\r' {
            out.truncate(len - 2);
        } else {
            out.truncate(len - 1);
        }
        return Ok(CsvTerminal::Newline);
    }
    debug_assert_eq!(out[len - 1], b',');
    out.truncate(len - 1);
    Ok(CsvTerminal::Comma)
}

fn process_header(
    ss: &mut StringStore,
    reader: &'_ mut impl BufRead,
    var_names: &mut Vec<StringStoreEntry>,
) -> Result<(), std::io::Error> {
    loop {
        let chunk = reader.fill_buf()?;
        if chunk.is_empty() {
            return Ok(());
        }
        let mut header_name = Vec::new();
        let terminal = if chunk[0] == b'"' {
            parse_quoted(reader, &mut header_name)?
        } else {
            parse_unquoted(reader, &mut header_name)?
        };
        let name = String::from_utf8(header_name).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "header name is invalid utf-8: {}",
                    String::from_utf8_lossy(e.as_bytes())
                ),
            )
        })?;
        var_names.push(ss.intern_moved(name));
        match terminal {
            CsvTerminal::Comma => {
                continue;
            }
            CsvTerminal::Newline | CsvTerminal::Eof => break,
        }
    }
    Ok(())
}

fn skip_header(reader: &mut impl BufRead) -> Result<(), std::io::Error> {
    let mut lcw = RememberLastCharacterWriter::default();
    loop {
        read_until_2(reader, &mut lcw, b'"', b'\n')?;
        if lcw.0.is_none() || lcw.0 == Some(b'\n') {
            return Ok(());
        }
        todo!("parse quoted");
    }
}

impl Operator for OpCsv {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "csv".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        self.var_names.len()
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        true
    }

    fn output_field_kind(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> typeline_core::operators::operator::OutputFieldKind {
        typeline_core::operators::operator::OutputFieldKind::Unconfigured
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        let op_id = sess.add_op(op_data_id, chain_id, offset_in_chain, span);
        if self.header {
            let reader = self
                .input
                .create_buf_reader()
                .and_then(|mut r| {
                    process_header(
                        &mut sess.string_store,
                        &mut r.aquire(),
                        &mut self.var_names,
                    )?;
                    Ok(r)
                })
                .map_err(|e| {
                    OperatorSetupError::new_s(
                        format!("failed to open file: {e}"),
                        op_id,
                    )
                })?;
            self.reader.lock().unwrap().replace(reader);
        } else {
            for i in 0..INITIAL_OUTPUT_COUNT {
                let var_name =
                    sess.string_store.intern_cloned(&format!("_{i}"));
                self.var_names.push(var_name);
            }
        }
        Ok(op_id)
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        ld: &mut LivenessData,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        output.flags.input_accessed = false;
        output.flags.non_stringified_input_access = false;

        for (i, var_name) in self.var_names.iter().enumerate() {
            ld.vars_to_op_outputs_map[ld.var_names[var_name]] =
                output.primary_output + OpOutputIdx::from_usize(i);
        }
    }

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) {
        for &vn in &self.var_names {
            ld.add_var_name(vn);
        }
    }

    fn on_liveness_computed(
        &mut self,
        _sess: &mut SessionData,
        _ld: &LivenessData,
        _op_id: OperatorId,
    ) {
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let actor_id = job
            .job_data
            .add_actor_for_tf_state_ignore_output_field(tf_state);

        let next_actor = job.job_data.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .action_buffer
            .borrow()
            .peek_next_actor_id();

        let input = if let Some(rdr) = self.reader.lock().unwrap().take() {
            Input::Running(rdr)
        } else {
            Input::NotStarted
        };

        let mut output_fields = vec![];
        for &name in &self.var_names {
            let field_id = job.job_data.field_mgr.add_field(
                &job.job_data.match_set_mgr,
                tf_state.match_set_id,
                ActorRef::Unconfirmed(next_actor),
            );
            output_fields.push(field_id);
            job.job_data.scope_mgr.insert_field_name(
                job.job_data.match_set_mgr.match_sets[tf_state.match_set_id]
                    .active_scope,
                name,
                field_id,
            );
        }

        tf_state.output_field = output_fields[0];

        TransformInstatiation::Single(Box::new(TfCsv {
            op: self,
            inserters: Default::default(),
            output_fields,
            input,
            lines_produced: 0,
            actor_id,
            additional_fields: StableVec::new(),
            group_iter: job
                .job_data
                .group_track_manager
                .claim_group_track_iter_ref(
                    tf_state.input_group_track_id,
                    next_actor,
                    IterKind::Transform(
                        job.job_data.tf_mgr.transforms.peek_claim_id(),
                    ),
                ),
        }))
    }
}

fn distribute_errors(
    inserters: &mut [VaryingTypeInserter<RefMut<'_, FieldData>>],
    operator_application_error: OperatorApplicationError,
) {
    for i in inserters {
        i.push_error(operator_application_error.clone(), 1, true, true);
    }
}

impl<'a> Transform<'a> for TfCsv<'a> {
    fn collect_out_fields(
        &self,
        _jd: &JobData,
        _tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        fields.extend_from_slice(&self.output_fields);
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (mut batch_size, mut ps) = jd.tf_mgr.claim_batch(tf_id);

        let tf = &jd.tf_mgr.transforms[tf_id];
        let ms_id = tf.match_set_id;
        let target_batch_size = tf.desired_batch_size;
        let op_id = tf.op_id.unwrap();

        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            self.output_fields.iter().copied(),
        );

        let mut additional_fields = self.additional_fields.borrow_container();
        let mut inserters = self.inserters.borrow_container();

        for &f in &self.output_fields {
            inserters.push(jd.field_mgr.get_varying_type_inserter(f));
        }
        let (reader, header_processed);

        let mut iter =
            jd.group_track_manager.lookup_group_track_iter_mut_from_ref(
                self.group_iter,
                &jd.match_set_mgr,
                self.actor_id,
            );

        let mut fields_produced = 0;

        if Some(iter.group_idx_stable()) == ps.group_to_truncate {
            self.input = Input::NotStarted;
            if self.lines_produced > 0 {
                iter.drop(1);
            }

            fields_produced += batch_size.min(iter.group_len_rem());
            batch_size = batch_size.saturating_sub(fields_produced);
            if !iter.try_next_group() || batch_size == 0 {
                jd.tf_mgr.submit_batch_ready_for_more(
                    tf_id,
                    fields_produced,
                    ps,
                );
                return;
            }
        }

        match &mut self.input {
            Input::NotStarted => match self.op.input.create_buf_reader() {
                Err(e) => {
                    let err =
                        OperatorApplicationError::new_s(e.to_string(), op_id);
                    distribute_errors(&mut inserters, err.clone());
                    jd.tf_mgr
                        .submit_batch_ready_for_more(tf_id, batch_size, ps);
                    self.input = Input::Error(err);
                    return;
                }
                Ok(r) => {
                    self.input = Input::Running(r);
                    let Input::Running(file) = &mut self.input else {
                        unreachable!()
                    };
                    reader = file;
                    header_processed = !self.op.header;
                }
            },
            Input::Running(buf_reader) => {
                reader = buf_reader;
                header_processed = true;
            }
            Input::Error(operator_application_error) => {
                distribute_errors(
                    &mut inserters,
                    operator_application_error.clone(),
                );
                jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
                return;
            }
        };

        let mut status = ReadStatus {
            lines_produced: 0,
            col_idx: 0,
            done: false,
        };

        let res = {
            let mut rdr = reader.aquire();

            let res = if !header_processed {
                skip_header(&mut rdr)
            } else {
                Ok(())
            };

            res.and_then(|_| {
                read_in_lines(
                    &mut rdr,
                    &mut inserters,
                    &additional_fields,
                    CsvReadOptions {
                        unused_fields: &self.op.unused_fields,
                        disable_quotes: self.op.disable_quotes,
                        prefix_nulls: iter.field_pos(),
                        lines_max: target_batch_size,
                    },
                    &mut status,
                )
            })
        };

        match res {
            Ok(()) => {
                if status.col_idx != 0 {
                    debug_assert!(status.done);
                    for i in status.col_idx..inserters.len() {
                        inserters[i].push_null(1, true);
                    }
                    status.lines_produced += 1;
                }

                if status.lines_produced == 0 {
                    if status.done && self.lines_produced != 0 {
                        iter.drop(1);
                    }
                } else {
                    iter.dup(status.lines_produced - usize::from(status.done));
                    iter.next_n_fields(status.lines_produced);
                }
                iter.store_iter(self.group_iter.iter_id);

                jd.tf_mgr
                    .unclaim_batch_size(tf_id, batch_size - fields_produced);
                ps.next_batch_ready = !status.done;
                ps.input_done = status.done;
                jd.tf_mgr.submit_batch_ready_for_more(
                    tf_id,
                    status.lines_produced,
                    ps,
                );

                if status.done {
                    self.input = Input::NotStarted;
                }
            }
            Err(io_error) => {
                let err = OperatorApplicationError::new_s(
                    format!(
                        "{}:{}:{} {}",
                        self.op.input.target_path(),
                        self.lines_produced + status.lines_produced,
                        status.col_idx,
                        io_error
                    ),
                    op_id,
                );
                for (idx, ins) in inserters.iter_mut().enumerate() {
                    let count = if status.col_idx == 0 || status.col_idx < idx
                    {
                        1
                    } else {
                        2
                    };
                    ins.push_error(err.clone(), count, true, true);
                }
                drop(iter);
            }
        }
        self.lines_produced += status.lines_produced;
        drop(inserters);

        if !additional_fields.is_empty() {
            let actor = ActorRef::Unconfirmed(self.actor_id + ActorId::one());

            let mut ssm = jd.session_data.string_store.write().unwrap();
            for ins in additional_fields.iter_mut() {
                let field_id = jd.field_mgr.add_field_with_data(
                    &jd.match_set_mgr,
                    ms_id,
                    actor,
                    std::mem::take(&mut ins.data.borrow_mut()),
                );
                self.output_fields.push(field_id);
                jd.scope_mgr.insert_field_name(
                    jd.match_set_mgr.match_sets[ms_id].active_scope,
                    ssm.intern_moved(std::mem::take(&mut ins.name)),
                    field_id,
                );
            }
            additional_fields.clear();
        }
    }
}

fn read_in_lines<'a>(
    reader: &mut impl BufRead,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    additional_fields: &'a StableVec<PendingField>,
    opts: CsvReadOptions,
    status: &mut ReadStatus,
) -> Result<(), std::io::Error> {
    let mut offset;
    let mut buffer: &[u8];
    let mut newline = false;
    let mut post_element = false;
    'refill: loop {
        buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            status.done = true;
            return Ok(());
        }
        offset = 0;
        'newline: loop {
            if newline {
                newline = false;
                let remaining_inserters = &mut inserters[status.col_idx..];
                if post_element {
                    for i in remaining_inserters {
                        i.push_null(1, true);
                    }
                    post_element = false;
                } else {
                    if let Some(i) = remaining_inserters.first_mut() {
                        i.push_str("", 1, true, true);
                    }
                    for i in remaining_inserters.iter_mut().skip(1) {
                        i.push_null(1, true);
                    }
                }
                status.col_idx = 0;
                status.lines_produced += 1;
                if status.lines_produced == opts.lines_max {
                    reader.consume(offset);
                    status.done = reader.fill_buf()?.is_empty();
                    return Ok(());
                }
                if offset == buffer.len() {
                    reader.consume(offset);
                    continue 'refill;
                }
            }
            let mut c = buffer[offset];

            if c == b'\r' {
                offset += 1;
                if offset == buffer.len() {
                    buffer = reader.fill_buf()?;
                    if buffer.is_empty() {
                        status.done = true;
                        return Ok(());
                    }
                    offset = 0;
                }
                c = buffer[offset];
                if c != b'\n' {
                    reader.consume(offset);
                    if insert_unquoted_from_stream(
                        &mut inserters[status.col_idx],
                        Some(b'\r'),
                        None,
                        reader,
                        &mut newline,
                    )? {
                        status.done = true;
                        return Ok(());
                    }
                    continue 'refill;
                }
            }
            if c == b'\n' {
                offset += 1;
                newline = true;
                continue;
            }

            loop {
                if !opts.disable_quotes && c == b'"' {
                    // todo: parse quoted text
                    unimplemented!();
                }

                let Some(end_index) = memchr2(b',', b'\n', &buffer[offset..])
                else {
                    break;
                };
                let cell_end = offset + end_index;
                let mut val_end = cell_end;
                c = buffer[cell_end];

                if c == b'\n' {
                    newline = true;
                    if val_end > 0 && buffer[val_end - 1] == b'\r' {
                        val_end -= 1;
                    }
                }
                let val = &buffer[offset..val_end];
                offset = cell_end + 1;

                if status.col_idx >= inserters.len() {
                    add_inserter(
                        additional_fields,
                        inserters,
                        status,
                        opts.prefix_nulls,
                    );
                }
                let inserter = &mut inserters[status.col_idx];
                if opts.unused_fields.get(status.col_idx) == Some(&true) {
                    inserter.push_undefined(1, true);
                } else if let Ok(v) = lexical_core::parse::<i64>(val) {
                    inserter.push_int(v, 1, true, false);
                } else if let Ok(v) = std::str::from_utf8(val) {
                    inserter.push_str(v, 1, true, false);
                } else {
                    inserter.push_bytes(val, 1, true, false);
                }

                status.col_idx += 1;
                post_element = true;

                if newline {
                    continue 'newline;
                }

                if offset == buffer.len() {
                    reader.consume(offset);
                    continue 'refill;
                }
                c = buffer[offset];
            }
            let eof = insert_unquoted_from_stream(
                &mut inserters[status.col_idx],
                None,
                Some(offset),
                reader,
                &mut newline,
            )?;
            status.col_idx += 1;
            if eof {
                status.done = true;
                return Ok(());
            }
            post_element = true;
            continue 'refill;
        }
    }
}

#[cold]
fn add_inserter<'a>(
    additional_fields: &'a StableVec<PendingField>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    status: &ReadStatus,
    prefix_nulls: usize,
) {
    additional_fields.push(PendingField {
        name: usize_to_str(inserters.len()).to_string(),
        data: RefCell::default(),
    });
    inserters.push(VaryingTypeInserter::new(
        additional_fields.last().unwrap().data.borrow_mut(),
    ));
    inserters[status.col_idx]
        .push_null(prefix_nulls + status.lines_produced, false);
}

#[cold]
fn insert_unquoted_from_stream<R: BufRead>(
    inserter: &mut VaryingTypeInserter<RefMut<FieldData>>,
    prefix_byte: Option<u8>,
    tail_of_reader: Option<usize>,
    reader: &mut R,
    newline: &mut bool,
) -> Result<bool, std::io::Error> {
    let mut stream = inserter.bytes_insertion_stream(1);
    if let Some(c) = prefix_byte {
        let _ = stream.write_all(std::slice::from_ref(&c));
    }
    if let Some(reader_tail) = tail_of_reader {
        let buf = reader.fill_buf()?;
        let _ = stream.write_all(&buf[reader_tail..]);
        let len = buf.len();
        reader.consume(len);
    }
    if read_until_2(reader, &mut stream, b',', b'\n')? == 0 {
        return Ok(true);
    }
    let buf = stream.get_inserted_data();
    let mut l = buf.len();
    let last = buf[l - 1];
    let mut eof = false;
    *newline = last == b'\n';
    if *newline && l > 1 && buf[l - 2] == b'\r' {
        l -= 2;
    } else if *newline || last == b',' {
        l -= 1;
    } else {
        eof = true;
    }
    if let Ok(buf) = str::from_utf8(&buf[..l]) {
        if let Ok(v) = buf.parse::<i64>() {
            stream.abort();
            inserter.push_int(v, 1, true, false);
        } else {
            stream.truncate(l);
            unsafe {
                stream.commit_as_text();
            }
        }
    } else {
        stream.truncate(l);
        stream.commit();
    }
    Ok(eof)
}

pub fn create_op_csv(
    input: ReadableTarget,
    header: bool,
    disable_quotes: bool,
) -> Box<dyn Operator> {
    Box::new(OpCsv {
        input,
        header,
        unused_fields: Vec::new(),
        var_names: Vec::new(),
        disable_quotes,
        reader: Mutex::new(None),
    })
}

pub fn create_op_csv_from_file(
    input_file: impl Into<PathBuf>,
    header: bool,
    disable_quotes: bool,
) -> Box<dyn Operator> {
    create_op_csv(
        ReadableTarget::File(input_file.into()),
        header,
        disable_quotes,
    )
}

pub fn parse_op_csv(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Option<Box<dyn Operator>>, TypelineError> {
    let (flags, args) = expr.split_flags_arg(false);
    if args.len() != 1 {
        return Err(expr.error_require_exact_positional_count(1).into());
    }
    let mut header = false;
    let mut disable_quotes = false;
    // TODO: this is non exhaustive.
    // add proper, generalized cli parsing code ala CLAP
    if let Some(flags) = flags {
        if flags.get("-h").is_some() {
            header = true;
        }
        if flags.get("-r").is_some() {
            disable_quotes = true;
        }
    }
    Ok(Some(create_op_csv_from_file(
        args[0].try_into_text(expr.op_name, sess)?.to_string(),
        header,
        disable_quotes,
    )))
}
