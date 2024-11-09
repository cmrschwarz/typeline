use core::str;
use std::{
    borrow::BorrowMut,
    cell::{RefCell, RefMut},
    io::{BufRead, Write},
    path::PathBuf,
};

use memchr::memchr2;
use scr_core::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorDataId, OperatorId,
            OperatorOffsetInChain, PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
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
    scr_error::ScrError,
    smallbox,
    utils::{
        indexing_type::IndexingType,
        int_string_conversions::usize_to_str,
        stable_vec::StableVec,
        string_store::StringStoreEntry,
        temp_vec::TransmutableContainer,
        test_utils::{read_until_2, read_until_match},
    },
};

pub struct OpCsv {
    header: bool,
    disable_quotes: bool,
    var_names: Vec<StringStoreEntry>,
    unused_fields: Vec<bool>,
    // TODO: add form that takes this from input
    input: ReadableTarget,
}

pub enum Input<'a> {
    NotStarted,
    Running(AnyBufReader<'a>),
    Error(OperatorApplicationError),
}

pub struct TfCsv<'a> {
    op: &'a OpCsv,
    input: Input<'a>,
    output_fields: Vec<FieldId>,
    inserters: Vec<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    additional_fields: StableVec<RefCell<FieldData>>,
    lines_produced: usize,
    actor_id: ActorId,
    group_iter: GroupTrackIterRef,
}

// HACK
// TODO: proper dynamic field management
const INITIAL_OUTPUT_COUNT: usize = 6;

impl Operator for OpCsv {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "csv".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        INITIAL_OUTPUT_COUNT
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        for i in 0..INITIAL_OUTPUT_COUNT {
            let var_name = sess.string_store.intern_cloned(&format!("_{i}"));
            self.var_names.push(var_name);
        }
        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        true
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
        for i in 0..INITIAL_OUTPUT_COUNT {
            ld.add_var_name(self.var_names[i]);
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
        let actor_id = job.job_data.add_actor_for_tf_state(tf_state);

        let next_actor = job.job_data.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .action_buffer
            .borrow()
            .peek_next_actor_id();

        let mut output_fields = vec![tf_state.output_field];
        for _ in 1..INITIAL_OUTPUT_COUNT {
            output_fields.push(job.job_data.field_mgr.add_field(
                &job.job_data.match_set_mgr,
                tf_state.match_set_id,
                ActorRef::Unconfirmed(next_actor),
            ));
        }
        for (i, &field_id) in output_fields.iter().enumerate() {
            job.job_data.scope_mgr.insert_field_name(
                job.job_data.match_set_mgr.match_sets[tf_state.match_set_id]
                    .active_scope,
                self.var_names[i],
                field_id,
            );
        }

        TransformInstatiation::Single(TransformData::Custom(smallbox!(
            TfCsv {
                op: self,
                inserters: Default::default(),
                output_fields,
                input: Input::NotStarted,
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
                            job.job_data.tf_mgr.transforms.peek_claim_id()
                        )
                    ),
            }
        )))
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
    fn display_name(&self) -> DefaultTransformName {
        "csv".into()
    }

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

        let mut additional_inserters =
            self.additional_fields.borrow_container();
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

        match read_in_lines(
            reader.aquire(),
            &mut inserters,
            &additional_inserters,
            &self.op.unused_fields,
            !header_processed,
            self.op.disable_quotes,
            iter.field_pos(),
            target_batch_size,
            &mut status,
        ) {
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

        if !additional_inserters.is_empty() {
            let actor = jd.field_mgr.fields[self.output_fields[0]]
                .borrow()
                .first_actor
                .get();
            let mut ssm = jd.session_data.string_store.write().unwrap();
            for ins in additional_inserters.iter_mut() {
                let field_id = jd.field_mgr.add_field_with_data(
                    &jd.match_set_mgr,
                    ms_id,
                    actor,
                    ins.borrow_mut().take(),
                );
                self.output_fields.push(field_id);
                jd.scope_mgr.insert_field_name(
                    jd.match_set_mgr.match_sets[ms_id].active_scope,
                    ssm.intern_cloned(&usize_to_str(self.output_fields.len())),
                    field_id,
                );
            }
            additional_inserters.clear();
        }
    }
}

struct ReadStatus {
    lines_produced: usize,
    col_idx: usize,
    done: bool,
}
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn read_in_lines<'a, R: BufRead>(
    mut reader: R,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    additional_fields: &'a StableVec<RefCell<FieldData>>,
    unused_fields: &[bool],
    process_header: bool,
    disable_quotes: bool,
    prefix_nulls: usize,
    lines_max: usize,
    status: &mut ReadStatus,
) -> Result<(), std::io::Error> {
    if process_header {
        read_until_match(&mut reader, &mut std::io::empty(), |buf| {
            memchr::memchr(b'\n', buf)
        })?;
    }
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
                if status.lines_produced == lines_max {
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
                        &mut reader,
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
                if !disable_quotes && c == b'"' {
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

                let inserter = &mut inserters[status.col_idx];
                if unused_fields.get(status.col_idx) == Some(&true) {
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

                if status.col_idx >= inserters.len() {
                    add_inserter(
                        additional_fields,
                        inserters,
                        status,
                        prefix_nulls,
                    );
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
                &mut reader,
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
    additional_fields: &'a StableVec<RefCell<FieldData>>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    status: &ReadStatus,
    prefix_nulls: usize,
) {
    additional_fields.push(RefCell::default());
    inserters.push(VaryingTypeInserter::new(
        additional_fields.last().unwrap().borrow_mut(),
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
) -> OperatorData {
    OperatorData::Custom(smallbox!(OpCsv {
        input,
        header,
        unused_fields: Vec::new(),
        var_names: Vec::new(),
        disable_quotes,
    }))
}

pub fn create_op_csv_from_file(
    input_file: impl Into<PathBuf>,
    header: bool,
    disable_quotes: bool,
) -> OperatorData {
    create_op_csv(
        ReadableTarget::File(input_file.into()),
        header,
        disable_quotes,
    )
}

pub fn parse_op_csv(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Option<OperatorData>, ScrError> {
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
        args[0].stringify_as_text(expr.op_name, sess)?.to_string(),
        header,
        disable_quotes,
    )))
}
