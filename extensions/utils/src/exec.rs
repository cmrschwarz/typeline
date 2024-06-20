use std::{
    collections::VecDeque,
    ffi::{OsStr, OsString},
    io::{Read, Write},
    process::{Child, Command},
    sync::Arc,
};

use bstr::ByteSlice;
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
        field_value::FieldValueKind,
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
        int_string_conversions::i64_to_str,
        maybe_text::MaybeText,
        string_store::{StringStoreEntry, INVALID_STRING_STORE_ENTRY},
        universe::Universe,
    },
};

#[derive(Clone)]
pub struct OpExec {
    fmt_parts: Vec<FormatPart>,
    refs_str: Vec<Option<String>>,
    refs_idx: Vec<Option<StringStoreEntry>>,
    fmt_arg_part_ends: Vec<usize>,
    stderr_field_name: StringStoreEntry,
    exit_code_field_name: StringStoreEntry,
}

struct CommandArgs {
    stdin_sv: Option<StreamValueId>,
    args: Vec<OsString>,
    stdin_data: Vec<u8>,
    error: Option<OperatorApplicationError>,
}

struct RunningCommand {
    stdin_data: Vec<u8>,
    stdin_offset: usize,
    proc: Child,
    stdout_sv_id: StreamValueId,
    stderr_sv_id: StreamValueId,
    exit_code_sv_id: StreamValueId,
}

type RunningCommandIdx = usize;

pub struct TfExec<'a> {
    op: &'a OpExec,
    iters: Vec<FieldIterRef>,
    command_args: Vec<CommandArgs>,
    running_command_ids: VecDeque<RunningCommandIdx>,
    running_commands: Universe<RunningCommandIdx, RunningCommand>,
    stderr_field: FieldId,
    exit_code_field: FieldId,
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
        self.stderr_field_name = string_store.intern_cloned("stderr");
        self.exit_code_field_name = string_store.intern_cloned("exit_code");
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

        drop(ab);
        let stderr_field = jd.field_mgr.add_field(
            &mut jd.match_set_mgr,
            tf_state.match_set_id,
            Some(self.stderr_field_name),
            actor_ref,
        );
        let exit_code_field = jd.field_mgr.add_field(
            &mut jd.match_set_mgr,
            tf_state.match_set_id,
            Some(self.exit_code_field_name),
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
                },
                exit_code_field
            }
        )))
    }
}

impl<'a> TfExec<'a> {
    fn push_text(&mut self, cmd_idx: usize, arg_idx: usize, text: &str) {
        if arg_idx == 0 {
            // TODO: maybe consider converting the encoding on non unix?
            self.command_args[cmd_idx]
                .stdin_data
                .extend_from_slice(text.as_bytes());
            return;
        }
        self.command_args[cmd_idx].args[arg_idx - 1].push(text);
    }
    #[cfg_attr(target_family = "unix", allow(unused))]
    fn push_bytes(
        &mut self,
        op_id: OperatorId,
        cmd_idx: usize,
        mut arg_idx: usize,
        data: &[u8],
    ) {
        if arg_idx == 0 {
            self.command_args[cmd_idx]
                .stdin_data
                .extend_from_slice(data);
            return;
        }
        arg_idx -= 1;
        #[cfg(target_family = "unix")]
        {
            let os_str =
                <OsStr as std::os::unix::ffi::OsStrExt>::from_bytes(data);
            self.command_args[cmd_idx].args[arg_idx].push(os_str);
            return;
        }
        #[allow(unreachable_code)]
        {
            let cmd_args = &mut self.command_args[cmd_idx];
            if cmd_args.error.is_some() {
                return;
            }
            match data.to_os_str() {
                Ok(data) => {
                    cmd_args.args[arg_idx].push(data);
                }
                Err(e) => {
                    let err = if arg_idx == 0 {
                        OperatorApplicationError::new(
                            "invalid utf-8 in program name",
                            op_id,
                        )
                    } else {
                        OperatorApplicationError::new_s(
                            format!("invalid utf-8 in cli argument {arg_idx}"),
                            op_id,
                        )
                    };
                    cmd_args.error = Some(err);
                }
            }
        }
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
                #[expand((T, ITER) in [
                    (TextInline, RefAwareInlineTextIter),
                    (TextBuffer, RefAwareTextBufferIter),
                ])]
                FieldValueSlice::T(text) => {
                    for v in ITER::from_range(&range, text).unfold_rl() {
                        self.push_text(cmd_idx, arg_idx, v);
                        cmd_idx += 1;
                    }
                }
                #[expand((T, ITER) in [
                    (BytesInline, RefAwareInlineBytesIter),
                    (BytesBuffer, RefAwareBytesBufferIter),
                ])]
                FieldValueSlice::T(bytes) => {
                    for v in ITER::from_range(&range, bytes).unfold_rl() {
                        self.push_bytes(op_id, cmd_idx, arg_idx, v);
                        cmd_idx += 1;
                    }
                }
                FieldValueSlice::Int(ints) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, ints)
                    {
                        let v = i64_to_str(false, *v);
                        for _ in 0..rl {
                            self.push_bytes(
                                op_id,
                                cmd_idx,
                                arg_idx,
                                v.as_bytes(),
                            );
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
                        let mut buf = MaybeText::default();

                        match v.format_raw(
                            &mut buf,
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
                                    if let Some(text) = buf.as_str() {
                                        self.push_text(cmd_idx, arg_idx, text);
                                    } else {
                                        self.push_bytes(
                                            op_id,
                                            cmd_idx,
                                            arg_idx,
                                            buf.as_bytes(),
                                        );
                                    }
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
                stdin_data: Vec::new(),
                args: vec![OsString::new(); self.op.fmt_arg_part_ends.len()],
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
                            self.push_bytes(op_id, i, arg_idx + 1, bytes);
                        }
                    }
                    FormatPart::TextLiteral(text) => {
                        for i in 0..batch_size {
                            self.push_text(i, arg_idx + 1, text);
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

        let mut exit_code_field =
            jd.field_mgr.fields[self.exit_code_field].borrow_mut();
        let mut exit_code_inserter =
            exit_code_field.iter_hall.varying_type_inserter();

        for cmd_idx in 0..batch_size {
            if let Some(e) = self.command_args[cmd_idx].error.take() {
                stderr_inserter.push_error(e.clone(), 1, true, false);
                stdout_inserter.push_error(e.clone(), 1, true, false);
                exit_code_inserter.push_error(e, 1, true, false);
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
                    let exit_code_stream =
                        jd.sv_mgr.claim_stream_value(StreamValue::new_empty(
                            Some(StreamValueDataType::SingleValue(
                                FieldValueKind::Int,
                            )),
                            StreamValueBufferMode::Stream,
                        ));
                    let cmd_id = self.running_commands.claim_with_value(
                        RunningCommand {
                            proc,
                            stdout_sv_id: stdout_stream,
                            stderr_sv_id: stderr_stream,
                            exit_code_sv_id: exit_code_stream,
                            stdin_data: std::mem::take(&mut ca.stdin_data),
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
                    exit_code_inserter.push_stream_value_id(
                        exit_code_stream,
                        1,
                        true,
                        false,
                    );
                }
                Err(e) => {
                    let e =
                        OperatorApplicationError::new_s(e.to_string(), op_id);
                    stderr_inserter.push_error(e.clone(), 1, true, false);
                    stdout_inserter.push_error(e.clone(), 1, true, false);
                    exit_code_inserter.push_error(e, 1, true, false);
                }
            }
        }
        jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
    }

    fn stream_producer_update(
        &mut self,
        jd: &mut JobData<'a>,
        tf_id: TransformId,
    ) {
        let batch_size = 1024; // TODO: configure properly
        let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();
        for &cmd_id in &self.running_command_ids {
            let cmd = &mut self.running_commands[cmd_id];
            let (stdout_sv, stderr_sv, exit_code_sv) =
                jd.sv_mgr.stream_values.three_distinct_mut(
                    cmd.stdout_sv_id,
                    cmd.stderr_sv_id,
                    cmd.exit_code_sv_id,
                );

            let mut stdout_inserter =
                stdout_sv.data_inserter(cmd.stdout_sv_id, batch_size, true);
            let mut stderr_inserter =
                stderr_sv.data_inserter(cmd.stdout_sv_id, batch_size, true);
            let mut exit_code_inserter = exit_code_sv.data_inserter(
                cmd.exit_code_sv_id,
                batch_size,
                true,
            );

            let mut res = Ok(None);

            if let Some(stdin) = &mut cmd.proc.stdin {
                if cmd.stdin_data.len() > cmd.stdin_offset {
                    match stdin.write(&cmd.stdin_data[cmd.stdin_offset..]) {
                        Ok(0) => {
                            cmd.proc.stdin.take();
                        }
                        Ok(n) => {
                            cmd.stdin_offset += n;
                        }
                        Err(e) => {
                            res = Err(e);
                        }
                    }
                }
            }
            if res.is_ok() {
                if let Some(stdout) = &mut cmd.proc.stdout {
                    res = match stdout_inserter
                        .with_bytes_buffer(|buf| stdout.read(buf))
                    {
                        Ok(0) => cmd.proc.try_wait(),
                        Ok(_n) => Ok(None),
                        Err(e) => Err(e),
                    }
                }
            }
            if res.is_ok() {
                if let Some(stderr) = &mut cmd.proc.stderr {
                    res = match stderr_inserter
                        .with_bytes_buffer(|buf| stderr.read(buf))
                    {
                        Ok(0) => cmd.proc.try_wait(),
                        Ok(_n) => Ok(None),
                        Err(e) => Err(e),
                    }
                }
            }
            let mut res = res.map_err(|e| {
                OperatorApplicationError::new_s(
                    format!(
                        "program communication failed with I/O Error: {e}"
                    ),
                    op_id,
                )
            });
            let mut done = false;
            if let Ok(Some(code)) = res {
                if !code.success() {
                    res = Err(OperatorApplicationError::new_s(
                        format!("program exited with code {code}"),
                        op_id,
                    ));
                }
                done = true;
            }
            if let Err(e) = res {
                let e = Some(Arc::new(e));
                stdout_inserter.propagate_error(&e);
                stderr_inserter.propagate_error(&e);
                exit_code_inserter.propagate_error(&e);
                done = true;
            }
            drop(stdout_inserter);
            drop(stderr_inserter);
            drop(exit_code_inserter);
            for sv_id in
                [cmd.stdout_sv_id, cmd.stderr_sv_id, cmd.exit_code_sv_id]
            {
                jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                if done {
                    jd.sv_mgr
                        .drop_field_value_subscription(sv_id, Some(tf_id));
                }
            }
            if done {
                self.running_commands.release(cmd_id);
            }
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
        fmt_arg_part_ends,
        stderr_field_name: INVALID_STRING_STORE_ENTRY,
        exit_code_field_name: INVALID_STRING_STORE_ENTRY
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
