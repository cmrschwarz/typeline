use std::{
    collections::VecDeque,
    ffi::{OsStr, OsString},
    io::{stdout, Read, Write},
    os::unix::ffi::{OsStrExt, OsStringExt},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
};

use metamatch::metamatch;
use scr_core::{
    chain::ChainId,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::{
            OperatorApplicationError, OperatorCreationError,
            OperatorSetupError,
        },
        format::{
            parse_format_string, FormatKey, FormatPart, FormatWidthSpec,
        },
        operator::{
            Operator, OperatorData, OperatorId, OperatorOffsetInChain,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorRef,
        field::{FieldId, FieldIterRef},
        field_data::{field_value_flags, FieldValueRepr},
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueRangeIter,
        formattable::{NumberFormat, RealizedFormatKey, TypeReprFormat},
        iter_hall::IterKind,
        iters::{FieldDataRef, FieldIter, UnfoldIterRunLength},
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
            RefAwareUnfoldIterRunLength,
        },
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueDataType,
            StreamValueId,
        },
    },
    smallbox,
    utils::{
        int_string_conversions::i64_to_str, string_store::StringStoreEntry,
        text_write::TextWriteIoAdapter, universe::Universe,
    },
};

#[derive(Clone, Default)]
pub struct OpExec {
    fmt_parts: Vec<FormatPart>,
    refs_str: Vec<Option<String>>,
    refs_idx: Vec<Option<StringStoreEntry>>,
    fmt_arg_part_ends: Vec<usize>,
}

struct CommandArgs {
    stdin_sv: Option<StreamValueId>,
    args: Vec<OsString>,
    error: Option<OperatorApplicationError>,
}

struct RunningCommand {
    stdin_data: Vec<u8>,
    stdin_offset: usize,
    proc: Child,
    stdout_sv_id: StreamValueId,
    stderr_sv_id: StreamValueId,
}

type RunningCommandIdx = usize;

pub struct TfExec<'a> {
    op: &'a OpExec,
    iters: Vec<FieldIterRef>,
    command_args: Vec<CommandArgs>,
    running_command_ids: VecDeque<RunningCommandIdx>,
    running_commands: Universe<RunningCommandIdx, RunningCommand>,
    stderr_field: FieldId,
    input_iter: FieldIterRef,
}

impl Operator for OpExec {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "exec".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        2
    }

    fn setup(
        &mut self,
        sess: &mut SessionData,
        _chain_id: ChainId,
        _op_id: OperatorId,
    ) -> Result<(), OperatorSetupError> {
        let mut string_store = sess.string_store.write().unwrap();
        for r in std::mem::take(&mut self.refs_str) {
            self.refs_idx.push(r.map(|r| string_store.intern_moved(r)));
        }
        Ok(())
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        sess: &SessionData,
        ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        op_offset_after_last_write: OperatorOffsetInChain,
        op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.may_dup_or_drop = false;
        // might be set to true again in the loop below
        access_flags.non_stringified_input_access = false;
        access_flags.input_accessed = false;
        for p in &self.fmt_parts {
            match p {
                FormatPart::ByteLiteral(_) | FormatPart::TextLiteral(_) => (),
                FormatPart::Key(fk) => {
                    let non_stringified = fk.min_char_count.is_some()
                        || fk.opts.add_plus_sign
                        || fk.opts.number_format != NumberFormat::Default
                        || fk.opts.zero_pad_numbers
                        || fk.opts.type_repr != TypeReprFormat::Regular;
                    if let Some(name) = self.refs_idx[fk.ref_idx as usize] {
                        ld.access_var(
                            sess,
                            op_id,
                            ld.var_names[&name],
                            op_offset_after_last_write,
                            non_stringified,
                        );
                    } else {
                        access_flags.input_accessed = true;
                        access_flags.non_stringified_input_access =
                            non_stringified;
                    }
                    if let Some(FormatWidthSpec::Ref(ws_ref)) =
                        fk.min_char_count
                    {
                        if let Some(name) = self.refs_idx[ws_ref as usize] {
                            ld.access_var(
                                sess,
                                op_id,
                                ld.var_names[&name],
                                op_offset_after_last_write,
                                true,
                            );
                        } else {
                            access_flags.input_accessed = true;
                            access_flags.non_stringified_input_access = true;
                        }
                    }
                }
            }
        }
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let jd = &mut job.job_data;
        let ab = jd.match_set_mgr.match_sets[tf_state.match_set_id]
            .action_buffer
            .borrow();

        let actor_ref = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        jd.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = actor_ref;
        let mut iters = Vec::new();

        let iter_kind =
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id());
        let ms = &jd.match_set_mgr.match_sets[tf_state.match_set_id];
        for &ref_idx in &self.refs_idx {
            let field_id = if let Some(name) = ref_idx {
                *ms.field_name_map.get(&name).unwrap_or(&ms.dummy_field)
            } else {
                tf_state.input_field
            };
            iters.push(FieldIterRef {
                field_id,
                iter_id: jd.field_mgr.claim_iter(field_id, iter_kind),
            });
        }
        let stderr_name = jd
            .session_data
            .string_store
            .write()
            .unwrap()
            .intern_cloned("stderr");

        drop(ab);
        let stderr_field = jd.field_mgr.add_field(
            &mut jd.match_set_mgr,
            tf_state.match_set_id,
            Some(stderr_name),
            actor_ref,
        );

        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfExec {
                op: self,
                iters,
                stderr_field,
                command_args: Vec::new(),
                running_command_ids: VecDeque::new(),
                running_commands: Universe::default(),
                input_iter: FieldIterRef {
                    iter_id: jd
                        .field_mgr
                        .claim_iter(tf_state.input_field, iter_kind),
                    field_id: tf_state.input_field
                }
            }
        )))
    }
}

impl<'a> TfExec<'a> {
    fn push_bytes(&mut self, cmd_idx: usize, arg_idx: usize, data: &[u8]) {
        let os_str = OsStr::from_bytes(data); // TODO: windows
        self.command_args[cmd_idx].args[arg_idx].push(os_str);
    }
    fn push_error(&mut self, cmd_idx: usize, err: OperatorApplicationError) {
        self.command_args[cmd_idx].error = Some(err)
    }
    fn add_iter_to_command_arg<'b, R: FieldDataRef<'b>>(
        &mut self,
        op_id: OperatorId,
        jd: &JobData,
        cmd_offset: usize,
        arg_idx: usize,
        _fmt_key: &FormatKey,
        iter: &mut AutoDerefIter<'b, FieldIter<'b, R>>,
    ) {
        let mut cmd_idx = cmd_offset;
        while let Some(range) = iter.typed_range_fwd(
            &jd.match_set_mgr,
            usize::MAX,
            field_value_flags::DEFAULT,
        ) {
            metamatch!(match range.base.data {
                FieldValueSlice::TextInline(text) => {
                    for v in RefAwareInlineTextIter::from_range(&range, text)
                        .unfold_rl()
                    {
                        self.push_bytes(cmd_idx, arg_idx, v.as_bytes());
                        cmd_idx += 1;
                    }
                }
                FieldValueSlice::BytesInline(bytes) => {
                    for v in RefAwareInlineBytesIter::from_range(&range, bytes)
                        .unfold_rl()
                    {
                        self.push_bytes(cmd_idx, arg_idx, v);
                        cmd_idx += 1;
                    }
                }
                FieldValueSlice::TextBuffer(text) => {
                    for v in RefAwareTextBufferIter::from_range(&range, text)
                        .unfold_rl()
                    {
                        self.push_bytes(cmd_idx, arg_idx, v.as_bytes());
                        cmd_idx += 1;
                    }
                }
                FieldValueSlice::BytesBuffer(bytes) => {
                    for v in RefAwareBytesBufferIter::from_range(&range, bytes)
                        .unfold_rl()
                    {
                        self.push_bytes(cmd_idx, arg_idx, v);
                        cmd_idx += 1;
                    }
                }
                FieldValueSlice::Int(ints) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, ints)
                    {
                        let v = i64_to_str(false, *v);
                        for _ in 0..rl {
                            self.push_bytes(cmd_idx, arg_idx, v.as_bytes());
                            cmd_idx += 1;
                        }
                    }
                }
                FieldValueSlice::Custom(custom_types) => {
                    for (v, rl) in RefAwareFieldValueRangeIter::from_range(
                        &range,
                        custom_types,
                    ) {
                        // TODO //PERF
                        let mut buf = Vec::new();

                        match v.format_raw(
                            &mut TextWriteIoAdapter(&mut buf),
                            &RealizedFormatKey::default(),
                        ) {
                            Err(e) => {
                                let op =  OperatorApplicationError::new_s(
                                        format!(
                                            "failed to stringify custom type '{}': {e}",
                                            v.type_name()
                                        ),
                                        op_id,
                                    );
                                for _ in 0..rl {
                                    self.push_error(cmd_idx, op.clone());
                                    cmd_idx += 1;
                                }
                            }

                            Ok(()) => {
                                for _ in 0..rl {
                                    self.push_bytes(cmd_idx, arg_idx, &buf);
                                    cmd_idx += 1;
                                }
                            }
                        }
                    }
                }

                FieldValueSlice::Error(errs) => {
                    for e in
                        RefAwareFieldValueRangeIter::from_range(&range, errs)
                            .unfold_rl()
                    {
                        self.push_error(cmd_idx, e.clone());
                        cmd_idx += 1;
                    }
                }

                FieldValueSlice::StreamValueId(svs) => {
                    if arg_idx == 0 {
                        for &sv in RefAwareFieldValueRangeIter::from_range(
                            &range, svs,
                        )
                        .unfold_rl()
                        {
                            self.command_args[cmd_idx].stdin_sv = Some(sv);
                            cmd_idx += 1;
                        }
                    } else {
                        todo!()
                    }
                }
                FieldValueSlice::BigInt(_)
                | FieldValueSlice::Float(_)
                | FieldValueSlice::Rational(_) => {
                    todo!();
                }
                #[expand(T in [Null, Undefined, Array, Object])]
                FieldValueSlice::T(_) => {
                    let e = OperatorApplicationError::new_s(
                        format!(
                            "unsupported input type {}",
                            FieldValueRepr::T.to_str()
                        ),
                        op_id,
                    );
                    for _ in 0..range.base.field_count {
                        self.push_error(cmd_idx, e.clone());
                        cmd_idx += 1;
                    }
                }
                FieldValueSlice::FieldReference(_)
                | FieldValueSlice::SlicedFieldReference(_) => unreachable!(),
            })
        }
    }

    fn push_args_from_field(
        &mut self,
        jd: &mut JobData,
        iter_ref: FieldIterRef,
        op_id: OperatorId,
        arg_idx: usize,
        fmt_key: &FormatKey,
    ) {
        let field = jd
            .field_mgr
            .get_cow_field_ref(&jd.match_set_mgr, iter_ref.field_id);
        let iter = jd.field_mgr.lookup_iter(
            iter_ref.field_id,
            &field,
            iter_ref.iter_id,
        );
        let mut iter =
            AutoDerefIter::new(&jd.field_mgr, iter_ref.field_id, iter);
        self.add_iter_to_command_arg(
            op_id,
            jd,
            0,
            arg_idx + 1,
            fmt_key,
            &mut iter,
        );
        jd.field_mgr.store_iter(
            iter_ref.field_id,
            iter_ref.iter_id,
            iter.into_base_iter(),
        );
    }
}

impl<'a> Transform<'a> for TfExec<'a> {
    fn display_name(&self) -> DefaultTransformName {
        "exec".into()
    }

    fn update(
        &mut self,
        jd: &mut JobData,
        tf_id: scr_core::operators::transform::TransformId,
    ) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let output_field_id = tf.output_field;
        let op_id = tf.op_id.unwrap();

        for _ in 0..batch_size {
            let command_args = CommandArgs {
                args: vec![
                    OsString::new();
                    self.op.fmt_arg_part_ends.len() + 1
                ],
                error: None,
                stdin_sv: None,
            };
            self.command_args.push(command_args);
        }

        self.push_args_from_field(
            jd,
            self.input_iter,
            op_id,
            0,
            &FormatKey::default(),
        );

        let mut part_idx = 0;

        for (arg_idx, &part_end) in
            self.op.fmt_arg_part_ends.iter().enumerate()
        {
            for fmt_part in &self.op.fmt_parts[part_idx..part_end] {
                match fmt_part {
                    FormatPart::ByteLiteral(bytes) => {
                        for i in 0..batch_size {
                            self.push_bytes(i, arg_idx + 1, bytes);
                        }
                    }
                    FormatPart::TextLiteral(text) => {
                        for i in 0..batch_size {
                            self.push_bytes(i, arg_idx + 1, text.as_bytes());
                        }
                    }
                    FormatPart::Key(fmt_key) => {
                        let iter_ref = self.iters[fmt_key.ref_idx as usize];
                        self.push_args_from_field(
                            jd,
                            iter_ref,
                            op_id,
                            arg_idx + 1,
                            fmt_key,
                        );
                    }
                }
            }
            part_idx = part_end;
        }

        let mut stdout_field =
            jd.field_mgr.fields[output_field_id].borrow_mut();
        let mut stdout_inserter =
            stdout_field.iter_hall.varying_type_inserter();

        let mut stderr_field =
            jd.field_mgr.fields[self.stderr_field].borrow_mut();
        let mut stderr_inserter =
            stderr_field.iter_hall.varying_type_inserter();

        for cmd_idx in 0..batch_size {
            if let Some(e) = self.command_args[cmd_idx].error.take() {
                stderr_inserter.push_error(e.clone(), 1, true, false);
                stdout_inserter.push_error(e, 1, true, false);
                continue;
            }
            let ca = &mut self.command_args[cmd_idx];
            match Command::new(&ca.args[1]).args(&ca.args[2..]).spawn() {
                Ok(proc) => {
                    let stdout_stream =
                        jd.sv_mgr.claim_stream_value(StreamValue::new_empty(
                            Some(StreamValueDataType::Bytes),
                            StreamValueBufferMode::Stream,
                        ));
                    let stderr_stream =
                        jd.sv_mgr.claim_stream_value(StreamValue::new_empty(
                            Some(StreamValueDataType::Bytes),
                            StreamValueBufferMode::Stream,
                        ));
                    let cmd_id = self.running_commands.claim_with_value(
                        RunningCommand {
                            proc,
                            stdout_sv_id: stdout_stream,
                            stderr_sv_id: stderr_stream,
                            stdin_data: std::mem::take(&mut ca.args[0])
                                .into_vec(),
                            stdin_offset: 0,
                        },
                    );
                    self.running_command_ids.push_back(cmd_id);
                    stderr_inserter.push_stream_value_id(
                        stdout_stream,
                        1,
                        true,
                        false,
                    );
                    stdout_inserter.push_stream_value_id(
                        stderr_stream,
                        1,
                        true,
                        false,
                    );
                }
                Err(e) => {
                    let e =
                        OperatorApplicationError::new_s(e.to_string(), op_id);
                    stderr_inserter.push_error(e.clone(), 1, true, false);
                    stdout_inserter.push_error(e, 1, true, false);
                }
            }
        }
    }

    fn stream_producer_update(
        &mut self,
        jd: &mut JobData<'a>,
        tf_id: TransformId,
    ) {
        let batch_size = 1024; // TODO: configure properly
        for &cmd_id in &self.running_command_ids {
            let cmd = &mut self.running_commands[cmd_id];

            let mut stdout_inserter = jd.sv_mgr.stream_values
                    [cmd.stdout_sv_id]
                    .data_inserter(cmd.stdout_sv_id, batch_size, true);
            let mut stderr_inserter = jd.sv_mgr.stream_values
                    [cmd.stdout_sv_id]
                    .data_inserter(cmd.stdout_sv_id, batch_size, true);

            let stdin_done = true;

            if let Some(stdin) =  cmd.proc.stdin {
                if cmd.stdin_data.len() > cmd.stdin_offset {
                    match  stdin.write(&cmd.stdin_data[cmd.stdin_offset..]) {
                        Ok(0) => {cmd.proc.stdin.take();},
                        Ok(n) => {
                            cmd.stdin_offset += n;
                            stdin_done = false;
                        },
                        Err(e) => {
                            let err = Arc::new(OperatorApplicationError)
                            stdout_inserter.propagate_error(error)
                        }
                    }

                }

            }

            let mut stdout_ok = true;
            if let Some(stdout) = &mut cmd.proc.stdout {

                match  stdout_inserter.with_bytes_buffer(|buf| stdout.read(buf)) {
                    Ok(0) => cmd.proc.try_wait()
                }

            }
            if res.is_ok() {

                res = stderr_inserter
                    .with_bytes_buffer(|buf| cmd.stdout.read(buf));
            }
            if let Err(e) = res {
                todo!()
            }
            if let Ok()
        }
    }

    fn handle_stream_value_update(
        &mut self,
        _jd: &mut JobData,
        _tf_id: TransformId,
        _sv_id: StreamValueId,
        _custom: usize,
    ) {
        todo!()
    }
}

pub fn parse_op_exec(
    args: Vec<Vec<u8>>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let mut parts = Vec::new();
    let mut refs = Vec::new();
    let mut fmt_arg_part_ends = Vec::new();
    for (i, arg) in args.iter().enumerate() {
        parse_format_string(arg, &mut refs, &mut parts).map_err(
            |(idx, msg)| OperatorCreationError {
                message: format!(
                    "exec format string arg {i} index {idx}: {msg}"
                )
                .into(),
                cli_arg_idx: arg_idx,
            },
        )?;
        fmt_arg_part_ends.push(parts.len());
    }

    Ok(OperatorData::Custom(smallbox!(OpExec {
        fmt_parts: parts,
        refs_idx: Vec::with_capacity(refs.len()),
        refs_str: refs,
        fmt_arg_part_ends
    })))
}

pub fn create_op_exec_from_strings(
    args: Vec<String>,
) -> Result<OperatorData, OperatorCreationError> {
    parse_op_exec(
        args.into_iter().map(|v| v.into_bytes()).collect::<Vec<_>>(),
        None,
    )
}
