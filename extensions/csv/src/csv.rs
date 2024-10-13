use std::{
    borrow::BorrowMut,
    cell::{RefCell, RefMut},
    io::{BufRead, Write},
    path::PathBuf,
};

use scr_core::{
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
        utils::readable::{AnyBufReader, ReadableTarget},
    },
    record_data::{
        action_buffer::{ActorId, ActorRef},
        field::{FieldId, FieldIterRef},
        field_action::FieldActionKind,
        field_data::FieldData,
        iter_hall::IterKind,
        iters::FieldIterator,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    tyson::TysonParser,
    utils::{
        int_string_conversions::usize_to_str, stable_vec::StableVec,
        temp_vec::TransmutableContainer, test_utils::read_until_2,
    },
};

pub struct OpCsv {
    header: bool,
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
    dummy_iter: FieldIterRef,
}

// HACK
// TODO: proper dynamic field management
const INITIAL_OUTPUT_COUNT: usize = 3;

impl Operator for OpCsv {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "csv".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        INITIAL_OUTPUT_COUNT
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
        _ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.input_accessed = false;
        access_flags.non_stringified_input_access = false;
        None
    }

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        sess: &SessionData,
        _op_id: OperatorId,
    ) {
        let mut ss = sess.string_store.write().unwrap();
        for i in 0..INITIAL_OUTPUT_COUNT {
            ld.add_var_name(ss.intern_cloned(&usize_to_str(i)));
        }
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let actor_id = job.job_data.add_actor_for_tf_state(tf_state);

        let ms =
            &mut job.job_data.match_set_mgr.match_sets[tf_state.match_set_id];
        let dummy_field = ms.dummy_field;
        let next_actor = ms.action_buffer.borrow().peek_next_actor_id();

        let mut output_fields = vec![tf_state.output_field];
        for _ in 1..INITIAL_OUTPUT_COUNT {
            output_fields.push(job.job_data.field_mgr.add_field(
                &job.job_data.match_set_mgr,
                tf_state.match_set_id,
                ActorRef::Unconfirmed(next_actor),
            ));
        }
        let mut ssm = job.job_data.session_data.string_store.write().unwrap();
        for (i, &field_id) in output_fields.iter().enumerate() {
            job.job_data.scope_mgr.insert_field_name(
                job.job_data.match_set_mgr.match_sets[tf_state.match_set_id]
                    .active_scope,
                ssm.intern_cloned(&usize_to_str(i)),
                field_id,
            );
        }

        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfCsv {
                op: self,
                inserters: Default::default(),
                output_fields,
                input: Input::NotStarted,
                lines_produced: 0,
                actor_id,
                additional_fields: StableVec::new(),
                dummy_iter: job.job_data.field_mgr.claim_iter_ref(
                    dummy_field,
                    // intentional. we want our own actions to affect this
                    // iter
                    actor_id,
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

    fn get_out_fields(
        &self,
        _tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        fields.extend_from_slice(&self.output_fields);
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
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

        if !header_processed {
            // TODO: process header
        }

        let mut lines_produced = self.lines_produced;
        let mut col_idx = 0;

        let field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, self.dummy_iter.field_id);
        let iter = jd.field_mgr.lookup_iter_from_ref(self.dummy_iter, &field);

        match read_in_lines(
            reader.aquire(),
            &additional_inserters,
            &mut inserters,
            &mut lines_produced,
            &mut col_idx,
            target_batch_size,
        ) {
            Ok(done) => {
                if col_idx != 0 {
                    debug_assert!(done);
                    for i in col_idx..inserters.len() {
                        inserters[i].push_null(1, true);
                    }
                    lines_produced += 1;
                }
                let produced_fields = lines_produced - self.lines_produced;
                let mut ab = jd.match_set_mgr.match_sets[ms_id]
                    .action_buffer
                    .borrow_mut();
                ab.begin_action_group(self.actor_id);
                ab.push_action(
                    FieldActionKind::Dup,
                    iter.get_next_field_pos(),
                    produced_fields,
                );
                ab.end_action_group();

                jd.tf_mgr.unclaim_batch_size(tf_id, batch_size);
                jd.tf_mgr.submit_batch_ready_for_more(
                    tf_id,
                    produced_fields,
                    ps,
                );

                if done {
                    self.input = Input::NotStarted;
                }
            }
            Err(io_error) => {
                let err = OperatorApplicationError::new_s(
                    format!(
                        "{}:{}:{} {}",
                        self.op.input.target_path(),
                        lines_produced,
                        col_idx,
                        io_error
                    ),
                    op_id,
                );
                for (idx, ins) in inserters.iter_mut().enumerate() {
                    let count =
                        if col_idx == 0 || col_idx < idx { 1 } else { 2 };
                    ins.push_error(err.clone(), count, true, true);
                }
            }
        }
        self.lines_produced = lines_produced;
        drop(inserters);
        drop(field);

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

fn read_in_lines<'a, R: BufRead>(
    mut reader: R,
    additional_fields: &'a StableVec<RefCell<FieldData>>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'a, FieldData>>>,
    lines_produced: &mut usize,
    col_idx: &mut usize,
    lines_max: usize,
) -> Result<bool, std::io::Error> {
    let max_line = *lines_produced + lines_max;
    loop {
        let mut c = 0;
        if reader.read(std::slice::from_mut(&mut c))? != 1 {
            return Ok(true);
        }
        if c == b'\r' {
            if reader.read(std::slice::from_mut(&mut c))? != 1 {
                return Ok(true);
            }
            if c != b'\n' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "expected \\n after \\r",
                ));
            }
        }
        if c == b'\n' {
            let remaining_inserters = &mut inserters[*col_idx..];
            if let Some(i) = remaining_inserters.first_mut() {
                i.push_str("", 1, true, true);
            }
            for i in remaining_inserters.iter_mut().skip(1) {
                i.push_null(1, true);
            }
            *col_idx = 0;
            *lines_produced += 1;
            if *lines_produced == max_line {
                return Ok(reader.fill_buf()?.is_empty());
            }
            continue;
        }
        let inserter = &mut inserters[*col_idx];
        *col_idx += 1;
        let newline;
        if c == b'"' {
            // todo: parse quoted text
            newline = false;
        } else {
            let mut stream = inserter.bytes_insertion_stream(1);
            let _ = stream.write_all(std::slice::from_mut(&mut c));
            if read_until_2(&mut reader, &mut stream, b',', b'\n')? == 0 {
                return Ok(true);
            }
            let buf = stream.get_inserted_data();
            let mut l = buf.len();
            let last = buf[l - 1];
            let mut eof = false;
            newline = last == b'\n';

            if newline && l > 1 && buf[l - 2] == b'\r' {
                l -= 2;
            } else if newline || last == b',' {
                l -= 1;
            } else {
                eof = true;
            }
            // HACK: this sucks. we will do weird stuff to quotes etc.
            match TysonParser::new(&buf[..l], true, None).parse_value() {
                Ok(v) => {
                    stream.abort();
                    inserter.push_field_value_unpacked(v, 1, true, true);
                }
                Err(_) => {
                    stream.truncate(l);
                    stream.commit()
                }
            }
            if eof {
                return Ok(true);
            }
        }
        if !newline {
            if *col_idx >= inserters.len() {
                additional_fields.push(RefCell::default());
                inserters.push(VaryingTypeInserter::new(
                    additional_fields.last().unwrap().borrow_mut(),
                ));
                inserters[*col_idx].push_null(*lines_produced, false);
            }
            continue;
        }
        for i in &mut inserters[*col_idx..] {
            i.push_null(1, true);
        }
        *col_idx = 0;
        *lines_produced += 1;
        if *lines_produced == max_line {
            return Ok(reader.fill_buf()?.is_empty());
        }
    }
}

pub fn create_op_csv(input: ReadableTarget, header: bool) -> OperatorData {
    OperatorData::Custom(smallbox!(OpCsv { input, header }))
}

pub fn create_op_csv_from_file(
    input_file: impl Into<PathBuf>,
    header: bool,
) -> OperatorData {
    OperatorData::Custom(smallbox!(OpCsv {
        input: ReadableTarget::File(input_file.into()),
        header
    }))
}
