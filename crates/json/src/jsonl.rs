use std::{cell::RefMut, io::BufRead, path::PathBuf, sync::Mutex};

use indexmap::IndexMap;
use serde::Deserializer;
use typeline_core::{
    chain::ChainId,
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
        VarLivenessSlotKind,
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
        field_data::{FieldData, FieldValueRepr},
        field_value::FieldValue,
        group_track::GroupTrackIterRef,
        iter_hall::IterKind,
        object::Object,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    typeline_error::TypelineError,
    utils::string_store::{StringStore, StringStoreEntry},
};

use indexland::{index_vec::IndexVec, nonmax::NonMaxUsize, Idx};

use indexland_utils::{
    stable_vec::StableVec, temp_vec::TransmutableContainer,
};

use crate::json_visitor::{
    InserterIndex, JsonlReadOptions, JsonlVisitor, PendingField,
};

#[derive(Default)]
pub struct JsonlOptions {
    pub use_null: bool,
    pub objectify: bool,
    pub disable_colum_opt: bool,
}

pub struct OpJsonl {
    var_names: Vec<StringStoreEntry>,
    accessed_fields: Vec<bool>,
    input: ReadableTarget,
    first_line: Option<FieldValue>,
    reader: Mutex<Option<AnyBufReader>>,
    opts: JsonlOptions,
}

pub enum Input {
    NotStarted,
    Running(AnyBufReader),
    Error(OperatorApplicationError),
}

pub struct TfJsonl<'a> {
    op: &'a OpJsonl,
    input: Input,
    output_fields: IndexMap<StringStoreEntry, FieldId>,
    inserters: IndexVec<
        InserterIndex,
        VaryingTypeInserter<RefMut<'static, FieldData>>,
    >,
    last_field_access: IndexVec<InserterIndex, Option<NonMaxUsize>>,
    dyn_access: bool,
    inserter_map: IndexMap<StringStoreEntry, InserterIndex>,
    additional_fields: StableVec<PendingField>,
    lines_produced: usize,
    actor_id: ActorId,
    group_iter: GroupTrackIterRef,
    line_buffer: Vec<u8>,
    first_line_processed: bool,
    zst_to_push: FieldValueRepr,
}

struct ReadStatus {
    lines_produced: usize,
    incomplete_line: bool,
    done: bool,
}

fn gather_field_names_from_first(
    ss: &mut StringStore,
    reader: &'_ mut impl BufRead,
    var_names: &mut Vec<StringStoreEntry>,
    line_buffer: &mut Vec<u8>,
) -> Result<FieldValue, std::io::Error> {
    reader.read_until(b'\n', line_buffer)?;

    let v = serde_json::from_slice(line_buffer)?;
    if let FieldValue::Object(obj) = &v {
        if let Object::KeysStored(obj) = &**obj {
            for key in obj.keys() {
                var_names.push(ss.intern_cloned(key));
            }
        }
    }
    line_buffer.clear();
    Ok(v)
}

impl Operator for OpJsonl {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "jsonl".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        self.var_names.len() + 1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        true
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
        let (reader, first_line) = self
            .input
            .create_buf_reader()
            .and_then(|mut r| {
                if self.opts.objectify {
                    Ok((r, None))
                } else {
                    let first_row = gather_field_names_from_first(
                        &mut sess.string_store,
                        &mut r.aquire(),
                        &mut self.var_names,
                        &mut Vec::new(),
                    )?;
                    Ok((r, Some(first_row)))
                }
            })
            .map_err(|e| {
                OperatorSetupError::new_s(
                    format!("failed to open file: {e}"),
                    op_id,
                )
            })?;
        self.first_line = first_line;
        self.reader.lock().unwrap().replace(reader);
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
                output.primary_output + OpOutputIdx::from_usize(i + 1);
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
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        // TODO: mirror this in csv
        let base = &sess.operator_bases[op_id];
        debug_assert_eq!(
            (base.outputs_end - base.outputs_start).into_usize(),
            self.var_names.len() + 1
        );
        for output in base.outputs_start.range_to(base.outputs_end) {
            let read = ld.op_outputs_data.get_slot(VarLivenessSlotKind::Reads)
                [output.into_usize()];
            self.accessed_fields
                .push(read || self.opts.disable_colum_opt);
        }
        // TODO: attempt to set dyn access to false
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

        let mut output_fields = IndexMap::new();
        let mut inserter_map = IndexMap::new();
        output_fields.insert(StringStoreEntry::MAX, tf_state.output_field);
        for &name in &self.var_names {
            let field_id = job.job_data.field_mgr.add_field(
                &job.job_data.match_set_mgr,
                tf_state.match_set_id,
                ActorRef::Unconfirmed(next_actor),
            );
            inserter_map.insert(name, InserterIndex(output_fields.len()));
            output_fields.insert(name, field_id);
            job.job_data.scope_mgr.insert_field_name(
                job.job_data.match_set_mgr.match_sets[tf_state.match_set_id]
                    .active_scope,
                name,
                field_id,
            );
        }

        tf_state.output_field = output_fields[0];

        let input = if let Some(rdr) = self.reader.lock().unwrap().take() {
            Input::Running(rdr)
        } else {
            Input::NotStarted
        };

        TransformInstatiation::Single(Box::new(TfJsonl {
            op: self,
            inserters: Default::default(),
            last_field_access: self
                .accessed_fields
                .iter()
                .map(|v| v.then_some(NonMaxUsize::ZERO))
                .collect(),
            output_fields,
            inserter_map,
            line_buffer: Vec::new(),
            input,
            lines_produced: 0,
            actor_id,
            additional_fields: StableVec::new(),
            dyn_access: true,
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
            first_line_processed: false,
            zst_to_push: if self.opts.use_null {
                FieldValueRepr::Null
            } else {
                FieldValueRepr::Undefined
            },
        }))
    }
}

fn distribute_errors<'a: 'b, 'b>(
    inserters: impl Iterator<
        Item = &'b mut VaryingTypeInserter<RefMut<'a, FieldData>>,
    >,
    operator_application_error: OperatorApplicationError,
) {
    for i in inserters {
        i.push_error(operator_application_error.clone(), 1, true, true);
    }
}

impl<'a> Transform<'a> for TfJsonl<'a> {
    fn collect_out_fields(
        &self,
        _jd: &JobData,
        _tf_state: &TransformState,
        fields: &mut Vec<FieldId>,
    ) {
        fields.extend(self.output_fields.values().copied());
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
            self.output_fields.values().copied(),
        );

        let mut additional_fields = self.additional_fields.borrow_container();
        let mut inserters = self.inserters.borrow_container();

        for &f in self.output_fields.values() {
            inserters.push(jd.field_mgr.get_varying_type_inserter(f));
        }
        let reader;

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
                    distribute_errors(inserters.iter_mut(), err.clone());
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
                }
            },
            Input::Running(buf_reader) => {
                reader = buf_reader;
            }
            Input::Error(operator_application_error) => {
                distribute_errors(
                    &mut inserters.iter_mut(),
                    operator_application_error.clone(),
                );
                jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
                return;
            }
        };

        let mut status = ReadStatus {
            lines_produced: 0,
            incomplete_line: false,
            done: false,
        };

        let res = {
            let mut rdr = reader.aquire();
            let mut ss = jd.session_data.string_store.write().unwrap();

            read_in_lines(
                &mut ss,
                &mut self.line_buffer,
                &mut rdr,
                &mut self.inserter_map,
                &mut inserters,
                &additional_fields,
                &mut self.last_field_access,
                JsonlReadOptions {
                    prefix_nulls: iter.field_pos(),
                    lines_produced_prev: self.lines_produced,
                    lines_max: target_batch_size,
                    dyn_access: self.dyn_access,
                    first_line_value: if self.first_line_processed {
                        None
                    } else {
                        self.op.first_line.as_ref()
                    },
                    zst_to_push: self.zst_to_push,
                    objectify: self.op.opts.objectify,
                },
                &mut status,
            )
        };
        if status.lines_produced > 0 {
            self.first_line_processed = true;
        }
        let line_count = self.lines_produced + status.lines_produced;

        match res {
            Ok(()) => {
                for (idx, line_idx) in
                    &mut self.last_field_access.iter_enumerated_mut()
                {
                    let gap = line_count
                        - line_idx
                            .map(|i| i.into_usize())
                            .unwrap_or(self.lines_produced);
                    inserters[idx].push_zst(self.zst_to_push, gap, true);
                    if line_idx.is_some() {
                        *line_idx = Some(NonMaxUsize::new(line_count).unwrap())
                    }
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
                        "{}:{} {}",
                        self.op.input.target_path(),
                        line_count,
                        io_error
                    ),
                    op_id,
                );
                for (idx, line_idx) in
                    &mut self.last_field_access.iter_enumerated_mut()
                {
                    let elem_count = line_idx
                        .map(|i| i.into_usize())
                        .unwrap_or(self.lines_produced);

                    let gap = (line_count
                        + usize::from(status.incomplete_line))
                        - elem_count;
                    if status.incomplete_line && gap == 0 {
                        continue;
                    }

                    if let Some(line_idx) = line_idx {
                        inserters[idx].push_zst(
                            self.zst_to_push,
                            gap - 1,
                            true,
                        );
                        if status.incomplete_line {
                            inserters[idx].push_error(
                                err.clone(),
                                1,
                                true,
                                true,
                            );
                        }
                        *line_idx = NonMaxUsize::new(
                            line_count + usize::from(status.incomplete_line),
                        )
                        .unwrap();
                    } else {
                        inserters[idx].push_zst(self.zst_to_push, gap, true);
                    }
                }

                drop(iter);
                ps.input_done = true; // TODO: this is not correct

                jd.tf_mgr.submit_batch_ready_for_more(
                    tf_id,
                    status.lines_produced + 1,
                    ps,
                );
            }
        }
        self.lines_produced +=
            status.lines_produced + usize::from(status.incomplete_line);
        drop(inserters);

        if !additional_fields.is_empty() {
            let actor = ActorRef::Unconfirmed(self.actor_id + ActorId::ONE);

            let mut ssm = jd.session_data.string_store.write().unwrap();
            for ins in additional_fields.iter_mut() {
                let name = ssm.intern_moved(std::mem::take(&mut ins.name));
                let field_id = jd.field_mgr.add_field_with_data(
                    &jd.match_set_mgr,
                    ms_id,
                    actor,
                    std::mem::take(&mut ins.data.borrow_mut()),
                );
                self.output_fields.insert(name, field_id);
                jd.scope_mgr.insert_field_name(
                    jd.match_set_mgr.match_sets[ms_id].active_scope,
                    name,
                    field_id,
                );
            }
            additional_fields.clear();
        }
    }
}
#[allow(clippy::too_many_arguments)]
fn read_in_lines<'a>(
    ss: &mut StringStore,
    line_buffer: &mut Vec<u8>,
    reader: &mut impl BufRead,
    inserter_map: &mut IndexMap<StringStoreEntry, InserterIndex>,
    inserters: &mut IndexVec<
        InserterIndex,
        VaryingTypeInserter<RefMut<'a, FieldData>>,
    >,
    additional_fields: &'a StableVec<PendingField>,
    last_field_access: &mut IndexVec<InserterIndex, Option<NonMaxUsize>>,
    opts: JsonlReadOptions,
    status: &mut ReadStatus,
) -> Result<(), std::io::Error> {
    let mut visitor = JsonlVisitor {
        ss,
        inserters,
        additional_fields,
        inserter_map,
        field_element_count: last_field_access,
        opts,
        total_lines_produced: opts.lines_produced_prev,
    };

    if let Some(value) = opts.first_line_value {
        let res = value.as_ref().deserialize_any(&mut visitor);
        if res.is_err() {
            status.incomplete_line = true;
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        visitor.total_lines_produced += 1;
    }

    for _ in 0..opts.lines_max {
        let count = reader.read_until(b'\n', line_buffer)?;
        if count == 0 {
            status.done = true;
            break;
        }
        let mut de = serde_json::Deserializer::from_slice(line_buffer);
        let res = de.deserialize_any(&mut visitor);
        line_buffer.clear();
        if let Err(e) = res {
            status.incomplete_line = true;
            status.lines_produced =
                visitor.total_lines_produced - opts.lines_produced_prev;
            return Err(e.into());
        }
        visitor.total_lines_produced += 1;
    }
    status.lines_produced =
        visitor.total_lines_produced - opts.lines_produced_prev;
    Ok(())
}

pub fn create_op_jsonl(
    input: ReadableTarget,
    opts: JsonlOptions,
) -> Box<dyn Operator> {
    Box::new(OpJsonl {
        input,
        accessed_fields: Vec::new(),
        var_names: Vec::new(),
        first_line: None,
        reader: Mutex::new(None),
        opts,
    })
}

pub fn create_op_jsonl_from_file(
    input_file: impl Into<PathBuf>,
    opts: JsonlOptions,
) -> Box<dyn Operator> {
    create_op_jsonl(ReadableTarget::File(input_file.into()), opts)
}

pub fn parse_op_jsonl(
    sess: &mut SessionSetupData,
    expr: CallExpr,
) -> Result<Option<Box<dyn Operator>>, TypelineError> {
    let (flags, args) = expr.split_flags_arg(false);
    if args.len() != 1 {
        return Err(expr.error_require_exact_positional_count(1).into());
    }
    let mut opts = JsonlOptions::default();
    // TODO: this is getting annoying, and error prone.
    // Add proper, generalized cli parsing code ala CLAP
    if let Some(flags) = flags {
        for (k, v) in flags {
            let FieldValue::Argument(arg) = v else {
                unreachable!()
            };
            match &**k {
                "-n" => {
                    opts.use_null = true;
                }
                "-o" => {
                    opts.objectify = true;
                }
                "--disable-column-opt" => {
                    opts.disable_colum_opt = true;
                }
                _ => {
                    return Err(expr
                        .error_flag_unsupported(k, arg.span)
                        .into());
                }
            }
        }
    }
    Ok(Some(create_op_jsonl_from_file(
        args[0].try_into_text(expr.op_name, sess)?.to_string(),
        opts,
    )))
}
