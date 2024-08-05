use std::{
    ffi::{OsStr, OsString},
    io::{ErrorKind, Read},
    os::unix::process::ExitStatusExt,
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
};

use bstr::ByteSlice;
use metamatch::metamatch;
use mio::{
    unix::pipe::{Receiver, Sender},
    Events, Poll,
};
use scr_core::{
    chain::ChainId,
    cli::{
        call_expr::{CallExpr, ParsedArgValue, Span},
        CliArgumentError,
    },
    context::SessionData,
    index_newtype,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        format::{
            access_format_key_refs, parse_format_string, FormatKey,
            FormatKeyRefData, FormatKeyRefId, FormatPart, FormatPartIndex,
        },
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorDataId, OperatorId,
            OperatorOffsetInChain, PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    options::{
        chain_settings::{
            SettingStreamBufferSize, SettingStreamSizeThreshold,
        },
        session_setup::SessionSetupData,
    },
    record_data::{
        action_buffer::ActorRef,
        bytes_insertion_stream::BytesInsertionStream,
        field::{FieldId, FieldIterRef},
        field_data::{FieldData, FieldValueRepr},
        field_value::FieldValueKind,
        field_value_ref::FieldValueSlice,
        field_value_slice_iter::FieldValueRangeIter,
        formattable::RealizedFormatKey,
        iter_hall::IterKind,
        iters::{FieldDataRef, FieldIter, FieldIterOpts, UnfoldIterRunLength},
        match_set::MatchSetManager,
        push_interface::PushInterface,
        ref_iter::{
            AutoDerefIter, RefAwareBytesBufferIter,
            RefAwareFieldValueRangeIter, RefAwareInlineBytesIter,
            RefAwareInlineTextIter, RefAwareTextBufferIter,
            RefAwareUnfoldIterRunLength,
        },
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataOffset, StreamValueDataType, StreamValueId,
            StreamValueManager, StreamValueUpdate,
        },
        varying_type_inserter::VaryingTypeInserter,
    },
    scr_error::ScrError,
    smallbox,
    utils::{
        index_vec::IndexVec,
        indexing_type::IndexingType,
        int_string_conversions::{f64_to_str, i64_to_str},
        maybe_text::MaybeText,
        string_store::{StringStoreEntry, INVALID_STRING_STORE_ENTRY},
        universe::{CountedUniverse, Universe},
    },
};

index_newtype! {
    pub struct ExecArgIdx(u32);
}

#[derive(Default, Clone)]
pub struct OpExecOpts {
    propagate_input: bool,
    // TODO: more stuff
}

pub struct OpExec {
    fmt_parts: IndexVec<FormatPartIndex, FormatPart>,
    refs: IndexVec<FormatKeyRefId, FormatKeyRefData>,
    fmt_arg_part_ends: IndexVec<ExecArgIdx, FormatPartIndex>,
    stderr_field_name: StringStoreEntry,
    exit_code_field_name: StringStoreEntry,
    opts: OpExecOpts,
}

struct CommandArgs {
    stdin_sv: Option<StreamValueId>,
    fake_stdin: bool,
    args: Vec<OsString>,
    error: Option<OperatorApplicationError>,
}

struct InStream {
    sender: Option<Sender>,
    token: Option<CommandOutputTokenId>,
    sv_id: Option<StreamValueId>,
    stdin_offset: StreamValueDataOffset,
}

struct OutStream {
    receiver: Option<Receiver>,
    token: Option<CommandOutputTokenId>,
    sv_id: Option<StreamValueId>,
    sv_appended: bool,
}

const STDOUT_IDX: usize = 0;
const STDERR_IDX: usize = 1;

struct RunningCommand {
    proc: Child,
    in_stream: InStream,
    out_streams: [OutStream; 2],
    exit_code_sv_id: StreamValueId,
    poll_requested: bool,
}
index_newtype! {
    struct RunningCommandIdx(u32);
}
type CommandOutputTokenId = usize;

#[derive(Clone, Copy)]
enum CommandStreamIdx {
    Stdin(RunningCommandIdx),
    Stdout(RunningCommandIdx),
    Stderr(RunningCommandIdx),
}

pub struct TfExec<'a> {
    op: &'a OpExec,
    token_universe: Universe<usize, CommandStreamIdx>,
    iters: IndexVec<FormatKeyRefId, FieldIterRef>,
    command_args: Vec<CommandArgs>,
    running_commands: CountedUniverse<RunningCommandIdx, RunningCommand>,
    stderr_field: FieldId,
    exit_code_field: FieldId,
    input_iter: Option<FieldIterRef>,
    stream_buffer_size: usize,
    stream_buffer_threshold: usize,
    poll: Poll,
    events: Option<Events>,
    commands_to_poll: Vec<RunningCommandIdx>,
}

impl CommandStreamIdx {
    fn id(&self) -> RunningCommandIdx {
        match *self {
            CommandStreamIdx::Stdin(id) => id,
            CommandStreamIdx::Stdout(id) => id,
            CommandStreamIdx::Stderr(id) => id,
        }
    }
}

impl Operator for OpExec {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "exec".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        2
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, ScrError> {
        for r in &mut self.refs {
            r.name_interned =
                r.name.as_ref().map(|n| sess.string_store.intern_cloned(n));
        }
        self.stderr_field_name = sess.string_store.intern_static("stderr");
        self.exit_code_field_name =
            sess.string_store.intern_cloned("exit_code");
        Ok(sess.add_op(op_data_id, chain_id, offset_in_chain, span))
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
        op_offset_after_last_write: OffsetInChain,
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
                    access_format_key_refs(
                        fk,
                        sess,
                        ld,
                        &self.refs,
                        op_id,
                        op_offset_after_last_write,
                        access_flags,
                    );
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
        let ms = &jd.match_set_mgr.match_sets[tf_state.match_set_id];
        let scope_id = ms.active_scope;
        let ab = ms.action_buffer.borrow();

        let actor_ref = ActorRef::Unconfirmed(ab.peek_next_actor_id());
        jd.field_mgr.fields[tf_state.output_field]
            .borrow()
            .first_actor
            .set(actor_ref);
        let mut iters = IndexVec::new();

        let iter_kind =
            IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id());
        let ms = &jd.match_set_mgr.match_sets[tf_state.match_set_id];
        for r in &self.refs {
            let field_id = if let Some(name) = r.name_interned {
                jd.scope_mgr
                    .lookup_field(ms.active_scope, name)
                    .unwrap_or(ms.dummy_field)
            } else {
                tf_state.input_field
            };
            iters.push(FieldIterRef {
                field_id,
                iter_id: jd.field_mgr.claim_iter_non_cow(field_id, iter_kind),
            });
        }

        drop(ab);
        let stderr_field = jd.field_mgr.add_field(
            &jd.match_set_mgr,
            tf_state.match_set_id,
            actor_ref,
        );
        jd.scope_mgr.insert_field_name(
            scope_id,
            self.stderr_field_name,
            stderr_field,
        );
        let exit_code_field = jd.field_mgr.add_field(
            &jd.match_set_mgr,
            tf_state.match_set_id,
            actor_ref,
        );
        jd.scope_mgr.insert_field_name(
            scope_id,
            self.exit_code_field_name,
            exit_code_field,
        );

        let stream_buffer_size = jd
            .get_scope_setting_or_default::<SettingStreamBufferSize>(scope_id);

        let stream_buffer_threshold = jd
            .get_scope_setting_or_default::<SettingStreamSizeThreshold>(
                scope_id,
            );

        let input_iter = if self.opts.propagate_input {
            Some(FieldIterRef {
                iter_id: jd
                    .field_mgr
                    .claim_iter_non_cow(tf_state.input_field, iter_kind),
                field_id: tf_state.input_field,
            })
        } else {
            None
        };

        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfExec {
                op: self,
                iters,
                token_universe: Universe::default(),
                stderr_field,
                command_args: Vec::new(),
                running_commands: CountedUniverse::default(),
                input_iter,
                exit_code_field,
                stream_buffer_size,
                stream_buffer_threshold,
                poll: Poll::new().unwrap(),
                events: Some(Events::with_capacity(64)),
                commands_to_poll: Vec::new(),
            }
        )))
    }
}

enum ProcessStart {
    Running {
        stdin: InStream,
        stdout: OutStream,
        stderr: OutStream,
        exit_stream: StreamValueId,
    },
    Done(ExitStatus),
}

impl<'a> TfExec<'a> {
    fn push_text(
        &mut self,
        sv_mgr: &mut StreamValueManager,
        cmd_idx: usize,
        arg_idx: usize,
        text: &str,
    ) {
        if arg_idx == 0 {
            // TODO: maybe consider converting the encoding on non unix?
            let sv_id = &mut self.command_args[cmd_idx].stdin_sv;
            if let Some(stdin_sv) = *sv_id {
                sv_mgr.stream_values[stdin_sv]
                    .data_inserter(stdin_sv, self.stream_buffer_size, false)
                    .append_text_copy(text);
            } else {
                *sv_id = Some(sv_mgr.stream_values.claim_with_value(
                    StreamValue::from_data(
                        Some(StreamValueDataType::MaybeText),
                        StreamValueData::from_string(text.to_owned()),
                        StreamValueBufferMode::Stream,
                        true,
                    ),
                ));
            }
            return;
        }
        self.command_args[cmd_idx].args[arg_idx - 1].push(text);
    }
    #[cfg_attr(target_family = "unix", allow(unused))]
    fn push_bytes(
        &mut self,
        sv_mgr: &mut StreamValueManager,
        op_id: OperatorId,
        cmd_idx: usize,
        mut arg_idx: usize,
        data: &[u8],
    ) {
        if arg_idx == 0 {
            let stdin_sv =
                if let Some(stdin_sv) = self.command_args[cmd_idx].stdin_sv {
                    stdin_sv
                } else {
                    sv_mgr.stream_values.claim_with_value(
                        StreamValue::new_empty(
                            Some(StreamValueDataType::Bytes),
                            StreamValueBufferMode::Stream,
                        ),
                    )
                };
            sv_mgr.stream_values[stdin_sv]
                .data_inserter(stdin_sv, self.stream_buffer_size, false)
                .append_bytes_copy(data);
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
            match <[u8] as bstr::ByteSlice>::to_os_str(data) {
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
        sv_mgr: &mut StreamValueManager,
        msm: &MatchSetManager,
        op_id: OperatorId,
        cmd_offset: usize,
        arg_idx: usize,
        _fmt_key: &FormatKey,
        iter: &mut AutoDerefIter<'b, FieldIter<'b, R>>,
    ) {
        let mut cmd_idx = cmd_offset;
        while let Some(range) =
            iter.typed_range_fwd(msm, usize::MAX, FieldIterOpts::default())
        {
            metamatch!(match range.base.data {
                #[expand((REP, ITER) in [
                    (TextInline, RefAwareInlineTextIter),
                    (TextBuffer, RefAwareTextBufferIter),
                ])]
                FieldValueSlice::REP(text) => {
                    for v in ITER::from_range(&range, text).unfold_rl() {
                        self.push_text(sv_mgr, cmd_idx, arg_idx, v);
                        cmd_idx += 1;
                    }
                }
                #[expand((REP, ITER) in [
                    (BytesInline, RefAwareInlineBytesIter),
                    (BytesBuffer, RefAwareBytesBufferIter),
                ])]
                FieldValueSlice::REP(bytes) => {
                    for v in ITER::from_range(&range, bytes).unfold_rl() {
                        self.push_bytes(sv_mgr, op_id, cmd_idx, arg_idx, v);
                        cmd_idx += 1;
                    }
                }

                #[expand((REP, CONV_FN) in [
                    (Int, i64_to_str(false, *v)),
                    (Float, f64_to_str(*v)),
                ])]
                FieldValueSlice::REP(ints) => {
                    for (v, rl) in
                        FieldValueRangeIter::from_range(&range, ints)
                    {
                        let v = CONV_FN;
                        for _ in 0..rl {
                            self.push_bytes(
                                sv_mgr,
                                op_id,
                                cmd_idx,
                                arg_idx,
                                v.as_bytes(),
                            );
                            cmd_idx += 1;
                        }
                    }
                }
                FieldValueSlice::BigInt(_)
                | FieldValueSlice::BigRational(_) => {
                    todo!();
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
                                        self.push_text(
                                            sv_mgr, cmd_idx, arg_idx, text,
                                        );
                                    } else {
                                        self.push_bytes(
                                            sv_mgr,
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
                            let ca = &mut self.command_args[cmd_idx];
                            ca.stdin_sv = Some(sv);
                            ca.fake_stdin = false;
                            cmd_idx += 1;
                        }
                    } else {
                        todo!()
                    }
                }

                #[expand(REP in [Null, Undefined, Array, Object, Argument])]
                FieldValueSlice::REP(_) => {
                    let e = OperatorApplicationError::new_s(
                        format!(
                            "unsupported input type {}",
                            FieldValueRepr::REP.to_str()
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
            &mut jd.sv_mgr,
            &jd.match_set_mgr,
            op_id,
            0,
            arg_idx,
            fmt_key,
            &mut iter,
        );
        jd.field_mgr.store_iter(
            iter_ref.field_id,
            iter_ref.iter_id,
            iter.into_base_iter(),
        );
    }

    fn insert_out_stream(
        &mut self,
        sv_mgr: &mut StreamValueManager,
        stdout_bis: BytesInsertionStream,
    ) -> StreamValueId {
        let stream = sv_mgr.claim_stream_value(StreamValue::new_empty(
            Some(StreamValueDataType::Bytes),
            StreamValueBufferMode::Stream,
        ));
        sv_mgr.stream_values[stream]
            .data_inserter(stream, self.stream_buffer_size, false)
            .append_bytes_copy(stdout_bis.get_inserted_data());

        stdout_bis.abort();

        stream
    }

    #[cfg(target_family = "unix")]
    fn setup_proc_streams(
        &mut self,
        tf_id: TransformId,
        sv_mgr: &mut StreamValueManager,
        proc: &mut Child,
        stdin_sv: Option<StreamValueId>,
        fake_stdin_sv: bool,
        stdout_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        stderr_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        exit_code_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        command_idx: RunningCommandIdx,
    ) -> Result<ProcessStart, std::io::Error> {
        use std::io::ErrorKind;

        use mio::{Interest, Token};
        use scr_core::record_data::stream_value::StreamValueDataOffset;

        let sbt = self.stream_buffer_threshold as u64;

        let mut stdin = proc.stdin.take().map(mio::unix::pipe::Sender::from);

        let mut token_stdin = None;
        let mut stdin_offset = StreamValueDataOffset::default();

        if let Some(input_sv) = stdin_sv {
            if !fake_stdin_sv {
                sv_mgr.subscribe_to_stream_value(
                    input_sv,
                    tf_id,
                    command_idx.into_usize(),
                    false,
                    true,
                )
            }

            let sv = &mut sv_mgr.stream_values[input_sv];
            let mut iter = sv.data_iter(StreamValueDataOffset {
                values_consumed: 0,
                current_value_offset: 0,
            });

            match std::io::copy(&mut iter, stdin.as_mut().unwrap()) {
                Ok(_) => {
                    if iter.is_end() && sv.done {
                        stdin.take();
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => (),
                Err(e) => return Err(e),
            };

            stdin_offset = iter.get_offset();
        }

        if let Some(stdin) = stdin.as_mut() {
            let stdin_tok = self
                .token_universe
                .claim_with_value(CommandStreamIdx::Stdin(command_idx));
            token_stdin = Some(stdin_tok);
            self.poll.registry().register(
                stdin,
                Token(stdin_tok),
                Interest::WRITABLE,
            )?;
        }

        let mut stdout =
            mio::unix::pipe::Receiver::from(proc.stdout.take().unwrap());
        stdout.set_nonblocking(true)?;

        let mut stderr =
            mio::unix::pipe::Receiver::from(proc.stderr.take().unwrap());
        stderr.set_nonblocking(true)?;

        let mut stdout_bis = stdout_inserter.bytes_insertion_stream(1);
        let stdout_bytes =
            match std::io::copy(&mut (&mut stdout).take(sbt), &mut stdout_bis)
            {
                Ok(n) => n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => 0,
                Err(e) => return Err(e),
            };

        let mut stderr_bis = stderr_inserter.bytes_insertion_stream(1);

        let stderr_bytes =
            match std::io::copy(&mut (&mut stderr).take(sbt), &mut stderr_bis)
            {
                Ok(n) => n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => 0,
                Err(e) => return Err(e),
            };

        if stdout_bytes != sbt || stderr_bytes != sbt {
            if let Some(exit_code) = proc.try_wait()? {
                stdout_bis.commit();
                stderr_bis.commit();
                exit_code_inserter.push_int(
                    exit_code.into_raw() as i64,
                    1,
                    true,
                    false,
                );
                return Ok(ProcessStart::Done(exit_code));
            }
        }

        let stdout_token = self
            .token_universe
            .claim_with_value(CommandStreamIdx::Stdout(command_idx));

        self.poll.registry().register(
            &mut stdout,
            Token(stdout_token),
            Interest::READABLE,
        )?;

        let stderr_token = self
            .token_universe
            .claim_with_value(CommandStreamIdx::Stderr(command_idx));
        self.poll.registry().register(
            &mut stderr,
            Token(stderr_token),
            Interest::READABLE,
        )?;

        let stdout_stream = self.insert_out_stream(sv_mgr, stdout_bis);
        let stderr_stream = self.insert_out_stream(sv_mgr, stderr_bis);
        let exit_stream = sv_mgr.claim_stream_value(StreamValue::new_empty(
            Some(StreamValueDataType::SingleValue(FieldValueKind::Int)),
            StreamValueBufferMode::Stream,
        ));

        Ok(ProcessStart::Running {
            stdin: InStream {
                sender: stdin,
                token: token_stdin,
                sv_id: stdin_sv,
                stdin_offset,
            },
            stdout: OutStream {
                receiver: Some(stdout),
                token: Some(stdout_token),
                sv_id: Some(stdout_stream),
                sv_appended: false,
            },
            stderr: OutStream {
                receiver: Some(stderr),
                token: Some(stderr_token),
                sv_id: Some(stderr_stream),
                sv_appended: false,
            },
            exit_stream,
        })
    }
    #[cfg(not(target_family = "unix"))]
    fn setup_out_streams(
        &mut self,
        sv_mgr: &mut StreamValueManager,
        _proc: &mut Child,
        stdout_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        stderr_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        _exit_code_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        _command_idx: RunningCommandIdx,
    ) -> Result<ProcessStart, std::io::Error> {
        let stdout_stream = sv_mgr.claim_stream_value(StreamValue::new_empty(
            Some(StreamValueDataType::Bytes),
            StreamValueBufferMode::Stream,
        ));
        stdout_inserter.push_stream_value_id(stdout_stream, 1, true, false);

        let stderr_stream = sv_mgr.claim_stream_value(StreamValue::new_empty(
            Some(StreamValueDataType::Bytes),
            StreamValueBufferMode::Stream,
        ));
        stderr_inserter.push_stream_value_id(stderr_stream, 1, true, false);

        let exit_stream = sv_mgr.claim_stream_value(StreamValue::new_empty(
            Some(StreamValueDataType::SingleValue(FieldValueKind::Int)),
            StreamValueBufferMode::Stream,
        ));
        stderr_inserter.push_stream_value_id(stderr_stream, 1, true, false);

        Ok(ProcessStart::Running {
            stdout: stdout_stream,
            stderr: stderr_stream,
            exit_stream,
        })
    }

    fn setup_proc(
        &mut self,
        tf_id: TransformId,
        sv_mgr: &mut StreamValueManager,
        cmd_idx: usize,
        stderr_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        stdout_inserter: &mut VaryingTypeInserter<&mut FieldData>,
        exit_code_inserter: &mut VaryingTypeInserter<&mut FieldData>,
    ) -> Result<(), std::io::Error> {
        let (mut proc, in_sv, fake_in_sv) = {
            let ca = &mut self.command_args[cmd_idx];
            let proc = Command::new(&ca.args[0])
                .args(&ca.args[1..])
                .stdin(if self.op.opts.propagate_input {
                    Stdio::piped()
                } else {
                    Stdio::null()
                })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            (proc, ca.stdin_sv, ca.fake_stdin)
        };

        let res = self.setup_proc_streams(
            tf_id,
            sv_mgr,
            &mut proc,
            in_sv,
            fake_in_sv,
            stdout_inserter,
            stderr_inserter,
            exit_code_inserter,
            self.running_commands.peek_claim_id(),
        );

        match res {
            Err(e) => {
                // last resort to not leave behind a mess.
                // should only happen in case of a very unfortunately timed
                // interrupt or something
                let _ = proc.kill();
                return Err(e);
            }
            Ok(ProcessStart::Running {
                stdin,
                stdout,
                stderr,
                exit_stream,
            }) => {
                stdout_inserter.push_stream_value_id(
                    stdout.sv_id.unwrap(),
                    1,
                    true,
                    false,
                );
                stderr_inserter.push_stream_value_id(
                    stderr.sv_id.unwrap(),
                    1,
                    true,
                    false,
                );
                exit_code_inserter.push_stream_value_id(
                    exit_stream,
                    1,
                    true,
                    false,
                );

                let cmd_idx =
                    self.running_commands.claim_with_value(RunningCommand {
                        proc,
                        in_stream: stdin,
                        out_streams: [stdout, stderr],
                        exit_code_sv_id: exit_stream,
                        poll_requested: true,
                    });
                // initial poll in case this command completed
                // before we managed to register the streams
                self.commands_to_poll.push(cmd_idx);
            }
            Ok(ProcessStart::Done(_status)) => (),
        }
        Ok(())
    }

    fn read_out_stream(
        &mut self,
        sv_mgr: &mut StreamValueManager,
        cmd_id: RunningCommandIdx,
        out_stream_idx: usize,
        op_id: OperatorId,
        read_to_end: bool,
    ) {
        let cmd = &mut self.running_commands[cmd_id];
        let out_stream = &mut cmd.out_streams[out_stream_idx];
        let Some(sv_id) = out_stream.sv_id else {
            return;
        };
        let mut sv = sv_mgr.stream_values[sv_id].data_inserter(
            sv_id,
            self.stream_buffer_size,
            !out_stream.sv_appended,
        );
        out_stream.sv_appended = true;
        let receiver = out_stream.receiver.as_mut().unwrap();
        let res = sv.with_bytes_buffer(|buf| {
            if read_to_end {
                receiver.read_to_end(buf)
            } else {
                receiver
                    .take(self.stream_buffer_size as u64)
                    .read_to_end(buf)
            }
        });

        if let Err(e) = &res {
            if e.kind() != ErrorKind::WouldBlock {
                let _ = self.poll.registry().deregister(receiver);
                self.token_universe
                    .release(out_stream.token.take().unwrap());
                out_stream.receiver.take();

                let e = Arc::new(OperatorApplicationError::new_s(
                    format!(
                        "{} communication failed with I/O Error: {e}",
                        match out_stream_idx {
                            STDOUT_IDX => "stderr",
                            STDERR_IDX => "stdout",
                            _ => unreachable!(),
                        }
                    ),
                    op_id,
                ));
                sv.set_error(e);
                drop(sv);
                sv_mgr.inform_stream_value_subscribers(sv_id);
                sv_mgr.drop_field_value_subscription(sv_id, None);
                out_stream.sv_id = None;
            }
        }
    }

    fn handle_input_stream(
        &mut self,
        tf_id: TransformId,
        sv_mgr: &mut StreamValueManager<'a>,
        cmd_id: RunningCommandIdx,
    ) -> Result<(), std::io::Error> {
        let stream = &mut self.running_commands[cmd_id].in_stream;
        let mut iter = sv_mgr.stream_values[stream.sv_id.unwrap()]
            .data_iter(stream.stdin_offset);
        match std::io::copy(&mut iter, stream.sender.as_mut().unwrap()) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::WouldBlock => (),
            Err(e) => {
                stream.sender.take();
                return Err(e);
            }
        }
        stream.stdin_offset = iter.get_offset();
        if iter.is_end() {
            let _ = self
                .poll
                .registry()
                .deregister(&mut stream.sender.take().unwrap());
            self.token_universe.release(stream.token.take().unwrap());
            sv_mgr.drop_field_value_subscription(
                stream.sv_id.take().unwrap(),
                Some(tf_id),
            )
        }
        Ok(())
    }

    fn propagate_process_failure(
        &mut self,
        jd: &mut JobData<'a>,
        cmd_id: RunningCommandIdx,
        op_id: OperatorId,
        e: std::io::Error,
    ) {
        let cmd = &mut self.running_commands[cmd_id];
        let e = Arc::new(OperatorApplicationError::new_s(
            format!("program communication failed with I/O Error: {e}"),
            op_id,
        ));
        if let Some(stdin_sv_id) = cmd.in_stream.sv_id {
            jd.sv_mgr.stream_values[stdin_sv_id].set_error(e.clone());
            jd.sv_mgr.drop_field_value_subscription(stdin_sv_id, None);
            if let Some(mut sender) = cmd.in_stream.sender.take() {
                let _ = self.poll.registry().deregister(&mut sender);
                self.token_universe.release(cmd.in_stream.token.unwrap());
            }
        }
        for out_stream_idx in 0..cmd.out_streams.len() {
            let out_stream = &mut cmd.out_streams[out_stream_idx];
            if let Some(sv_id) = out_stream.sv_id {
                jd.sv_mgr.stream_values[sv_id].set_error(e.clone());
                jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                jd.sv_mgr.drop_field_value_subscription(sv_id, None);
            }
            if let Some(mut rec) = out_stream.receiver.take() {
                let _ = self.poll.registry().deregister(&mut rec);
                self.token_universe
                    .release(out_stream.token.take().unwrap());
            }
        }
        jd.sv_mgr.stream_values[cmd.exit_code_sv_id].set_error(e);
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
                stdin_sv: None,
                fake_stdin: true,
                args: vec![OsString::new(); self.op.fmt_arg_part_ends.len()],
                error: None,
            };
            self.command_args.push(command_args);
        }

        if let Some(iter) = self.input_iter {
            self.push_args_from_field(
                jd,
                iter,
                op_id,
                0,
                &FormatKey::default(),
            );
        }

        let mut part_idx = FormatPartIndex::zero();

        for (arg_idx, &part_end) in
            self.op.fmt_arg_part_ends.iter().enumerate()
        {
            for fmt_part in &self.op.fmt_parts[part_idx..part_end] {
                match fmt_part {
                    FormatPart::ByteLiteral(bytes) => {
                        for i in 0..batch_size {
                            self.push_bytes(
                                &mut jd.sv_mgr,
                                op_id,
                                i,
                                arg_idx + 1,
                                bytes,
                            );
                        }
                    }
                    FormatPart::TextLiteral(text) => {
                        for i in 0..batch_size {
                            self.push_text(
                                &mut jd.sv_mgr,
                                i,
                                arg_idx + 1,
                                text,
                            );
                        }
                    }
                    FormatPart::Key(fmt_key) => {
                        let iter_ref = self.iters[fmt_key.ref_idx];
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
            match self.setup_proc(
                tf_id,
                &mut jd.sv_mgr,
                cmd_idx,
                &mut stderr_inserter,
                &mut stdout_inserter,
                &mut exit_code_inserter,
            ) {
                Ok(()) => (),
                Err(e) => {
                    let e = OperatorApplicationError::new_s(
                        format!(
                            "failed to execute `{}`: {}",
                            self.command_args[cmd_idx].args[0]
                                .to_string_lossy(),
                            e,
                        ),
                        op_id,
                    );
                    stderr_inserter.push_error(e.clone(), 1, true, false);
                    stdout_inserter.push_error(e.clone(), 1, true, false);
                    exit_code_inserter.push_error(e, 1, true, false);
                }
            }
        }

        if !self.running_commands.is_empty() {
            jd.tf_mgr.make_stream_producer(tf_id);
        }

        jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);

        self.command_args.clear();
    }

    #[cfg(target_family = "unix")]
    fn stream_producer_update(
        &mut self,
        jd: &mut JobData<'a>,
        tf_id: TransformId,
    ) {
        use std::time::Duration;

        use scr_core::record_data::{
            field_value::FieldValue, stream_value::StreamValueData,
        };

        let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();

        let mut events = self.events.take().unwrap();
        self.poll
            .poll(&mut events, Some(Duration::from_millis(1)))
            .expect("poll error");

        for e in &events {
            let token = e.token();
            let cmd_output = self.token_universe[token.0];
            let cmd_id = cmd_output.id();
            let cmd = &mut self.running_commands[cmd_id];
            if !cmd.poll_requested {
                self.commands_to_poll.push(cmd_id);
                cmd.poll_requested = true;
            }
            let out_stream_idx = match cmd_output {
                CommandStreamIdx::Stdout(_) => STDOUT_IDX,
                CommandStreamIdx::Stderr(_) => STDERR_IDX,
                CommandStreamIdx::Stdin(_) => {
                    if let Err(e) =
                        self.handle_input_stream(tf_id, &mut jd.sv_mgr, cmd_id)
                    {
                        self.propagate_process_failure(jd, cmd_id, op_id, e);
                    }
                    continue;
                }
            };
            cmd.out_streams[out_stream_idx].sv_appended = true;
            self.read_out_stream(
                &mut jd.sv_mgr,
                cmd_id,
                out_stream_idx,
                op_id,
                false,
            );
        }
        events.clear();
        self.events = Some(events);

        // HACK: this works around the fact that sometimes
        // we don't get any epoll results despite the process
        // ending
        // TODO: figure out a better way to do this
        for (idx, cmd) in self.running_commands.iter_enumerated_mut() {
            if !cmd.poll_requested {
                cmd.poll_requested = true;
                self.commands_to_poll.push(idx);
            }
        }

        while let Some(cmd_id) = self.commands_to_poll.pop() {
            let cmd = &mut self.running_commands[cmd_id];
            cmd.poll_requested = false;
            let mut done = false;
            match cmd.proc.try_wait() {
                Ok(None) => {}
                Ok(Some(status)) => {
                    jd.sv_mgr.stream_values[cmd.exit_code_sv_id]
                        .data_inserter(
                            cmd.exit_code_sv_id,
                            self.stream_buffer_size,
                            true,
                        )
                        .append(StreamValueData::Single(FieldValue::Int(
                            status.into_raw() as i64,
                        )));

                    for out_stream_idx in 0..cmd.out_streams.len() {
                        self.read_out_stream(
                            &mut jd.sv_mgr,
                            cmd_id,
                            out_stream_idx,
                            op_id,
                            true,
                        );
                    }
                    done = true;
                }
                Err(e) => {
                    self.propagate_process_failure(jd, cmd_id, op_id, e);
                    done = true;
                }
            }
            let cmd = &mut self.running_commands[cmd_id];
            for out_stream_idx in 0..cmd.out_streams.len() {
                let os = &mut cmd.out_streams[out_stream_idx];
                let appended = os.sv_appended;
                os.sv_appended = false;
                if let Some(sv_id) = os.sv_id {
                    if done {
                        jd.sv_mgr.stream_values[sv_id].mark_done();
                    }
                    if done || appended {
                        jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                    }
                    if done {
                        jd.sv_mgr.drop_field_value_subscription(sv_id, None);
                        if let Some(token) = os.token {
                            self.token_universe.release(token);
                        }
                        if let Some(mut stream) = os.receiver.take() {
                            let _ =
                                self.poll.registry().deregister(&mut stream);
                        }
                    }
                }
            }
            if !done {
                continue;
            }
            if let Some(token) = cmd.in_stream.token {
                self.token_universe.release(token);
            }
            if let Some(mut stream) = cmd.in_stream.sender.take() {
                let _ = self.poll.registry().deregister(&mut stream);
            }

            let cmd = &mut self.running_commands[cmd_id];
            jd.sv_mgr
                .inform_stream_value_subscribers(cmd.exit_code_sv_id);
            jd.sv_mgr
                .drop_field_value_subscription(cmd.exit_code_sv_id, None);

            self.running_commands.release(cmd_id);
        }

        if !self.running_commands.is_empty() {
            jd.tf_mgr.make_stream_producer(tf_id);
        }
    }

    #[cfg(not(target_family = "unix"))]
    fn stream_producer_update(
        &mut self,
        jd: &mut JobData<'a>,
        tf_id: TransformId,
    ) {
        let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();
        for cmd_id in self.running_commands.next_index_phys().range_from_zero()
        {
            let Some(cmd) = &mut self.running_commands.get_mut(cmd_id) else {
                continue;
            };
            let (stdout_sv, stderr_sv, exit_code_sv) =
                jd.sv_mgr.stream_values.three_distinct_mut(
                    cmd.stdout_sv_id,
                    cmd.stderr_sv_id,
                    cmd.exit_code_sv_id,
                );

            let mut stdout_inserter = stdout_sv.data_inserter(
                cmd.stdout_sv_id,
                self.stream_buffer_size,
                true,
            );
            let mut stderr_inserter = stderr_sv.data_inserter(
                cmd.stderr_sv_id,
                self.stream_buffer_size,
                true,
            );
            let mut exit_code_inserter = exit_code_sv.data_inserter(
                cmd.exit_code_sv_id,
                self.stream_buffer_size,
                true,
            );

            let mut res = Ok(None);

            let mut any_stream_running = false;

            if let Some(stdin) = &mut cmd.proc.stdin {
                if cmd.stdin_data.len() > cmd.stdin_offset {
                    match stdin.write(&cmd.stdin_data[cmd.stdin_offset..]) {
                        Ok(0) => {
                            cmd.proc.stdin.take();
                        }
                        Ok(n) => {
                            any_stream_running = true;
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
                    res = match stdout_inserter.with_bytes_buffer(|buf| {
                        stdout
                            .take(self.stream_buffer_size as u64)
                            .read_to_end(buf)
                    }) {
                        Ok(0) => {
                            cmd.proc.stdout.take();
                            Ok(None)
                        }
                        Ok(_n) => {
                            any_stream_running = true;
                            Ok(None)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
            if res.is_ok() {
                if let Some(stderr) = &mut cmd.proc.stderr {
                    res = match stderr_inserter.with_bytes_buffer(|buf| {
                        stderr
                            .take(self.stream_buffer_size as u64)
                            .read_to_end(buf)
                    }) {
                        Ok(0) => {
                            cmd.proc.stderr.take();
                            Ok(None)
                        }
                        Ok(_n) => {
                            any_stream_running = true;
                            Ok(None)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
            if !any_stream_running && res.is_ok() {
                res = cmd.proc.try_wait();
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
            if done {
                for ins in [
                    &mut stdout_inserter,
                    &mut stderr_inserter,
                    &mut exit_code_inserter,
                ] {
                    ins.stream_value().mark_done();
                }
            }
            drop(stdout_inserter);
            drop(stderr_inserter);
            drop(exit_code_inserter);
            for sv_id in
                [cmd.stdout_sv_id, cmd.stderr_sv_id, cmd.exit_code_sv_id]
            {
                jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                if done {
                    jd.sv_mgr.drop_field_value_subscription(sv_id, None);
                }
            }
            if done {
                self.running_commands.release(cmd_id);
            }
        }
        if !self.running_commands.is_empty() {
            jd.tf_mgr.make_stream_producer(tf_id);
        }
    }

    fn handle_stream_value_update(
        &mut self,
        jd: &mut JobData,
        svu: StreamValueUpdate,
    ) {
        jd.sv_mgr.stream_values[svu.sv_id]
            .set_subscriber_data_offset(svu.tf_id, svu.data_offset);
    }
}

fn append_exec_arg(
    arg_idx: usize,
    value: &[u8],
    span: Span,
    refs: &mut IndexVec<FormatKeyRefId, FormatKeyRefData>,
    parts: &mut IndexVec<FormatPartIndex, FormatPart>,
    fmt_arg_part_ends: &mut IndexVec<ExecArgIdx, FormatPartIndex>,
) -> Result<(), CliArgumentError> {
    parse_format_string(value.as_bytes(), refs, parts).map_err(
        |(i, msg)| {
            CliArgumentError::new_s(
                format!("exec format string arg {arg_idx} offset {i}: {msg}",),
                span,
            )
        },
    )?;
    fmt_arg_part_ends.push(parts.next_idx());
    Ok(())
}

pub fn parse_op_exec(expr: &CallExpr) -> Result<OperatorData, ScrError> {
    let mut parts = IndexVec::new();
    let mut refs = IndexVec::new();
    let mut fmt_arg_part_ends = IndexVec::new();
    let mut opts = OpExecOpts::default();
    for arg in expr.parsed_args_iter() {
        match arg.value {
            ParsedArgValue::Flag(flag) => {
                if flag == "-i" {
                    opts.propagate_input = true;
                    continue;
                }
                return Err(expr
                    .error_flag_unsupported(flag, arg.span)
                    .into());
            }
            ParsedArgValue::NamedArg { key, .. } => {
                return Err(expr
                    .error_named_args_unsupported(key, arg.span)
                    .into());
            }
            ParsedArgValue::PositionalArg { idx, value, .. } => {
                let Some(value) = value.text_or_bytes() else {
                    return Err(expr
                        .error_non_primitive_arg_unsupported(arg.span)
                        .into());
                };
                append_exec_arg(
                    idx,
                    value,
                    arg.span,
                    &mut refs,
                    &mut parts,
                    &mut fmt_arg_part_ends,
                )?;
            }
        }
    }

    Ok(OperatorData::Custom(smallbox!(OpExec {
        fmt_parts: parts,
        refs,
        fmt_arg_part_ends,
        opts,
        stderr_field_name: INVALID_STRING_STORE_ENTRY,
        exit_code_field_name: INVALID_STRING_STORE_ENTRY,
    })))
}

pub fn create_op_exec_from_strings<'a>(
    args: impl IntoIterator<Item = impl Into<&'a str>>,
) -> Result<OperatorData, OperatorCreationError> {
    create_op_exec_with_opts_from_strings(OpExecOpts::default(), args)
}

pub fn create_op_exec_with_opts_from_strings<'a>(
    opts: OpExecOpts,
    args: impl IntoIterator<Item = impl Into<&'a str>>,
) -> Result<OperatorData, OperatorCreationError> {
    let mut parts = IndexVec::new();
    let mut refs = IndexVec::new();
    let mut fmt_arg_part_ends = IndexVec::new();
    for (idx, value) in args.into_iter().enumerate() {
        append_exec_arg(
            idx,
            value.into().as_bytes(),
            Span::Generated,
            &mut refs,
            &mut parts,
            &mut fmt_arg_part_ends,
        )?;
    }

    Ok(OperatorData::Custom(smallbox!(OpExec {
        fmt_parts: parts,
        refs,
        fmt_arg_part_ends,
        opts,
        stderr_field_name: INVALID_STRING_STORE_ENTRY,
        exit_code_field_name: INVALID_STRING_STORE_ENTRY,
    })))
}
