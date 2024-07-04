use std::{
    collections::{HashMap, VecDeque},
    io::{stdout, Write},
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Condvar, Mutex, RwLock},
    thread::JoinHandle,
};

use smallvec::SmallVec;

#[cfg(feature = "repl")]
use {
    reedline::{
        default_emacs_keybindings, EditCommand, Emacs, FileBackedHistory,
        History, HistoryItem, KeyCode, KeyModifiers, Reedline, ReedlineEvent,
        Signal,
    },
    shlex::Shlex,
};

use crate::{
    chain::{Chain, ChainId},
    cli::CliOptions,
    extension::ExtensionRegistry,
    job::{Job, JobData},
    liveness_analysis::{self, LivenessData},
    operators::{
        aggregator::create_op_aggregate_raw,
        errors::OperatorSetupError,
        nop_copy::create_op_nop_copy,
        operator::{
            OffsetInChain, OperatorBase, OperatorData, OperatorDataId,
            OperatorId, OperatorOffsetInChain,
        },
    },
    options::{
        operator_base_options::{
            OperatorBaseOptions, OperatorBaseOptionsInterned,
        },
        setting::CliArgIdx,
    },
    record_data::{
        record_buffer::RecordBuffer, record_set::RecordSet,
        scope_manager::ScopeManager,
    },
    scr_error::ChainSetupError,
    utils::{
        identity_hasher::BuildIdentityHasher,
        index_vec::IndexVec,
        indexing_type::{IndexingType, IndexingTypeRange},
        maybe_boxed::MaybeBoxed,
        string_store::{StringStore, StringStoreEntry},
    },
};

pub struct JobDescription {
    pub operator: OperatorId,
    pub data: RecordSet,
}

pub struct VentureDescription {
    pub participans_needed: usize,
    pub starting_points: SmallVec<[OperatorId; 4]>,
    pub buffer: Arc<RecordBuffer>,
}

pub struct Venture<'a> {
    pub venture_id: usize,
    pub description: VentureDescription,
    pub source_job: Option<Box<Job<'a>>>,
}

pub struct SessionSettings {
    pub max_threads: usize,
    pub repl: bool,
    pub skipped_first_cli_arg: bool,
    pub debug_log_path: Option<PathBuf>,
}

pub struct SessionSetupData {
    pub settings: SessionSettings,
    pub scope_mgr: ScopeManager,
    pub chains: IndexVec<ChainId, Chain>,
    pub chain_labels: HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    pub operator_bases: IndexVec<OperatorId, OperatorBase>,
    pub operator_data: IndexVec<OperatorDataId, OperatorData>,
    pub string_store: StringStore,
    pub extensions: Arc<ExtensionRegistry>,
}

pub struct SessionData {
    pub settings: SessionSettings,
    pub scope_mgr: ScopeManager,
    pub chains: IndexVec<ChainId, Chain>,
    pub chain_labels: HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    pub operator_bases: IndexVec<OperatorId, OperatorBase>,
    pub operator_data: IndexVec<OperatorDataId, OperatorData>,
    pub cli_args: Option<IndexVec<CliArgIdx, Vec<u8>>>,
    pub string_store: RwLock<StringStore>,
    pub extensions: Arc<ExtensionRegistry>,
    pub success: AtomicBool,
}

pub struct Session {
    pub(crate) session_data: Arc<SessionData>,
    pub(crate) job_queue: VecDeque<JobDescription>,
    pub(crate) venture_queue: VecDeque<Venture<'static>>,
    pub(crate) terminate: bool,
    pub(crate) waiting_worker_threads: usize,
    pub(crate) total_worker_threads: usize,
    pub(crate) waiting_venture_participants: usize,
    pub(crate) venture_id_counter: usize,
    pub(crate) thread_join_handles: Vec<JoinHandle<()>>,
}

pub struct ContextData {
    pub(crate) session: Mutex<Session>,
    pub(crate) work_available: Condvar,
    pub(crate) worker_threads_finished: Condvar,
}

struct WorkerThread {
    pub ctx_data: Arc<ContextData>,
    // in case of panics, we want to increment the waiter counter
    // if this is false
    pub waiting: bool,
}

pub struct Context {
    session: Arc<SessionData>,
    main_thread: WorkerThread,
}

impl Drop for WorkerThread {
    fn drop(&mut self) {
        if !self.waiting {
            let mut sess = self.ctx_data.session.lock().unwrap();
            sess.waiting_worker_threads += 1;
            if sess.waiting_worker_threads == sess.total_worker_threads {
                self.ctx_data.worker_threads_finished.notify_one();
            }
        }
    }
}

impl WorkerThread {
    pub(crate) fn new(ctx_data: Arc<ContextData>) -> Self {
        Self {
            ctx_data,
            waiting: false,
        }
    }
    pub fn run_job<'a>(&mut self, mut job: impl MaybeBoxed<Job<'a>>) {
        match job.base_ref_mut().run(Some(&self.ctx_data)) {
            Ok(()) => (),
            Err(venture_desc) => {
                self.ctx_data.session.lock().unwrap().submit_venture(
                    venture_desc,
                    Some(job.boxed()),
                    &self.ctx_data,
                );
            }
        }
    }
    pub fn run_venture(
        &mut self,
        sess: &SessionData,
        start_op_id: OperatorId,
        buffer: Arc<RecordBuffer>,
    ) {
        let mut job = Job::new(sess);
        job.setup_venture(Some(&self.ctx_data), buffer, start_op_id);
        self.run_job(job)
    }
    pub fn run_job_from_desc(
        &mut self,
        sess: &SessionData,
        job_desc: JobDescription,
    ) {
        let mut job = Job::new(sess);
        job.setup_job(job_desc);
        self.run_job(job)
    }

    pub fn run(&mut self) {
        let mut sess_mgr = self.ctx_data.session.lock().unwrap();

        loop {
            if !sess_mgr.terminate {
                let waiting_venture_participants =
                    sess_mgr.waiting_venture_participants;
                if let Some(venture) = sess_mgr.venture_queue.front_mut() {
                    let source_job = venture.source_job.take();
                    let buffer = venture.description.buffer.clone();
                    let participants_needed =
                        venture.description.participans_needed;
                    let start_op_id = venture.description.starting_points
                        [waiting_venture_participants];
                    let sess;
                    sess_mgr.waiting_venture_participants += 1;
                    if sess_mgr.waiting_venture_participants
                        == participants_needed
                    {
                        sess_mgr.waiting_venture_participants = 0;
                        sess_mgr.venture_queue.pop_front();
                        sess_mgr.venture_id_counter =
                            sess_mgr.venture_id_counter.wrapping_add(1);
                        sess = sess_mgr.session_data.clone();
                        drop(sess_mgr);
                        self.ctx_data.work_available.notify_all();
                    } else {
                        let counter = sess_mgr.venture_id_counter;
                        loop {
                            if counter != sess_mgr.venture_id_counter {
                                sess = sess_mgr.session_data.clone();
                                drop(sess_mgr);
                                break;
                            }
                            sess_mgr = self
                                .ctx_data
                                .work_available
                                .wait(sess_mgr)
                                .unwrap();
                        }
                    }
                    if let Some(job) = source_job {
                        self.run_job(job);
                    } else {
                        self.run_venture(&sess, start_op_id, buffer);
                    }
                    sess_mgr = self.ctx_data.session.lock().unwrap();
                    continue;
                }
                if let Some(job) = sess_mgr.job_queue.pop_front() {
                    let sess = sess_mgr.session_data.clone();
                    drop(sess_mgr);
                    self.run_job_from_desc(&sess, job);
                    sess_mgr = self.ctx_data.session.lock().unwrap();
                    continue;
                }
            }
            sess_mgr.waiting_worker_threads += 1;
            self.waiting = true;
            if sess_mgr.waiting_worker_threads == sess_mgr.total_worker_threads
            {
                self.ctx_data.worker_threads_finished.notify_one();
            }
            if sess_mgr.terminate {
                return;
            }
            sess_mgr = self.ctx_data.work_available.wait(sess_mgr).unwrap();
            sess_mgr.waiting_worker_threads -= 1;
            self.waiting = false;
        }
    }
}

impl Session {
    #[allow(dead_code)] // TODO
    pub fn submit_job(
        &mut self,
        ctx_data: &Arc<ContextData>,
        job: JobDescription,
    ) {
        self.job_queue.push_back(job);
        // TODO: better check
        if self.session_data.settings.max_threads < self.total_worker_threads
            && self.waiting_worker_threads == 0
        {
            self.add_worker_thread(ctx_data);
        }
    }
    pub fn add_worker_thread(&mut self, ctx_data: &Arc<ContextData>) {
        let ctx_data = ctx_data.clone();
        self.total_worker_threads += 1;
        #[cfg(feature = "debug_logging")]
        eprintln!(
            "added worker thread: {} / {}",
            self.total_worker_threads, self.session_data.settings.max_threads
        );
        self.thread_join_handles.push(std::thread::spawn(move || {
            WorkerThread::new(ctx_data).run();
        }));
    }
    pub fn submit_venture<'a>(
        &mut self,
        desc: VentureDescription,
        starting_job: Option<Box<Job<'a>>>,
        ctx_data: &Arc<ContextData>,
    ) {
        // SAFETY: this assert ensures that we ourselves hold an Arc<Session>
        // for the Session that causes the lifetime restriction on this
        // JobSession before we ourselves drop the Arc<Session>, we
        // make sure that all ventures are dropped, see set_session
        assert!(starting_job
            .as_ref()
            .map(|js| std::ptr::eq(
                js.job_data.session_data,
                self.session_data.as_ref()
            ))
            .unwrap_or(true));
        let id = self.venture_id_counter;
        self.venture_id_counter = id.wrapping_add(1);
        let spawnable_thread_count =
            self.session_data.settings.max_threads - self.total_worker_threads;
        let threads_needed =
            desc.participans_needed.min(spawnable_thread_count);
        self.venture_queue.push_back(Venture {
            description: desc,
            venture_id: id,
            // SAFETY: see comment above for why transmuting this livetime is
            // justified
            source_job: unsafe {
                std::mem::transmute::<
                    Option<Box<Job<'a>>>,
                    Option<Box<Job<'static>>>,
                >(starting_job)
            },
        });
        for _ in 0..threads_needed {
            self.add_worker_thread(ctx_data);
        }
    }
    pub fn set_session(&mut self, sess: Arc<SessionData>) {
        assert!(self.job_queue.is_empty());
        assert!(self.venture_queue.is_empty());
        // SAFETY: the asserts above make sure that nobody is still refering to
        // the old session
        self.session_data = sess;
    }
}

impl Context {
    pub fn new(session: Arc<SessionData>) -> Self {
        let ctx_data = Arc::new(ContextData {
            work_available: Condvar::new(),
            worker_threads_finished: Condvar::new(),
            // we add 1 total / waiting thread for the main thread
            session: Mutex::new(Session {
                terminate: false,
                total_worker_threads: 1,
                venture_id_counter: 0,
                waiting_venture_participants: 0,
                waiting_worker_threads: 1,
                venture_queue: VecDeque::new(),
                job_queue: VecDeque::new(),
                session_data: session.clone(),
                thread_join_handles: Vec::new(),
            }),
        });
        Self {
            main_thread: WorkerThread::new(ctx_data),
            session,
        }
    }
    fn wait_for_worker_threads(&self) {
        let ctx = self.main_thread.ctx_data.as_ref();
        let mut sess_mgr = ctx.session.lock().unwrap();
        loop {
            if sess_mgr.waiting_worker_threads == sess_mgr.total_worker_threads
            {
                return;
            }
            sess_mgr = ctx.worker_threads_finished.wait(sess_mgr).unwrap();
        }
    }
    pub fn set_session(&mut self, mut session: SessionData) {
        if self.session.settings.max_threads < session.settings.max_threads {
            // TODO: we might want to lower ours instead?
            // but this is simple and prevents deadlocks for now
            session.settings.max_threads = self.session.settings.max_threads;
        }
        let new_sess = Arc::new(session);

        // PERF: this lock is completely pointless, we known nobody else
        // is using this right now. we could get rid of this using unsafe,
        // but it's probably not worth it
        let mut sess_mgr = self.main_thread.ctx_data.session.lock().unwrap();
        sess_mgr.set_session(new_sess.clone());
        self.session = new_sess;
    }
    pub fn get_session(&self) -> &SessionData {
        &self.session
    }
    pub fn run_job(&mut self, job: JobDescription) {
        self.main_thread.run_job_from_desc(&self.session, job);
        if self.session.settings.max_threads > 1 {
            self.wait_for_worker_threads();
        }
    }
    pub fn run_main_chain(&mut self, input_data: RecordSet) {
        self.run_job(self.session.construct_main_chain_job(input_data));
    }
    #[cfg(feature = "repl")]
    pub fn run_repl(&mut self, mut cli_opts: crate::cli::CliOptions) {
        use crate::{
            cli::{call_expr::Span, parse_cli},
            options::session_options::SessionOptions,
            repl_prompt::ScrPrompt,
            scr_error::ScrError,
        };
        debug_assert!(cli_opts.allow_repl);
        if !self.session.has_no_command(&cli_opts) {
            self.run_main_chain(RecordSet::default());
        }
        cli_opts.skip_first_arg = false;
        let mut history = Box::<FileBackedHistory>::default();
        if let Some(args) = &self.session.cli_args {
            history
                .save(HistoryItem::from_command_line(
                    &args.iter().map(|arg| String::from_utf8_lossy(arg)).fold(
                        String::new(),
                        |mut s, a| {
                            s.push(' ');
                            // HACK // TODO: error handling
                            s.push_str(&shlex::try_quote(&a).unwrap());
                            s
                        },
                    )[1..],
                ))
                .unwrap();
        }
        let mut keybindings = default_emacs_keybindings();
        // prevent CTRL + left/right form getting stuck on every sigil
        // still not ideal since this skips e.g. the equals sign, but better
        keybindings.add_binding(
            KeyModifiers::CONTROL,
            KeyCode::Left,
            ReedlineEvent::Edit(vec![EditCommand::MoveBigWordLeft {
                select: false,
            }]),
        );
        keybindings.add_binding(
            KeyModifiers::CONTROL,
            KeyCode::Right,
            ReedlineEvent::Edit(vec![
                EditCommand::MoveBigWordRightEnd { select: false },
                // move right one more character so we start typing *after*
                // the word, not before the last character
                EditCommand::MoveRight { select: false },
            ]),
        );
        let edit_mode = Emacs::new(keybindings);
        let mut line_editor = Reedline::create()
            .with_history(history)
            .with_edit_mode(Box::new(edit_mode));
        let prompt = ScrPrompt::default();

        loop {
            let sig = line_editor.read_line(&prompt);
            match sig {
                Ok(Signal::Success(buffer)) => {
                    let mut shlex = Shlex::new(&buffer);
                    let args =
                        shlex.by_ref().map(String::into_bytes).collect();
                    let mut exit_repl = false;
                    let sess = if shlex.had_error {
                        Err("failed to tokenize command line arguments"
                            .to_string())
                    } else {
                        let mut sess_opts = parse_cli(&cli_opts, args);
                        sess_opts = sess_opts.map(|mut opts| {
                            exit_repl = opts.repl.get() == Some(false)
                                || opts.exit_repl.get() == Some(true);
                            opts.repl.force_set(true, Span::Builtin);
                            opts
                        });
                        match sess_opts.and_then(SessionOptions::build_session)
                        {
                            Ok(sess) => Ok(sess),
                            Err(e) => match e.err {
                                ScrError::PrintInfoAndExitError(e) => {
                                    println!("{}", e.get_message());
                                    continue;
                                }
                                ScrError::MissingArgumentsError(_) => {
                                    continue;
                                }
                                _ => Err(e.contextualized_message),
                            },
                        }
                    };
                    match sess {
                        Ok(sess) => {
                            self.set_session(sess);
                            if !self.session.has_no_command(&cli_opts) {
                                self.run_main_chain(RecordSet::default());
                            }
                        }
                        Err(e) => {
                            eprintln!("\x1b[0;31mError:\x1b[0m {e}");
                            continue;
                        }
                    }
                    if exit_repl {
                        break;
                    }
                }
                Ok(Signal::CtrlC) => {
                    // eprintln!("^C");
                    continue;
                }
                Ok(Signal::CtrlD) => {
                    eprintln!("exit");
                    break;
                }
                Err(err) => {
                    eprintln!("REPL IO Error: {}", err);
                    break;
                }
            }
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if self.session.settings.max_threads <= 1 {
            return;
        }
        let ctx = self.main_thread.ctx_data.as_ref();
        let mut sess_mgr = ctx.session.lock().unwrap();
        sess_mgr.terminate = true;
        ctx.work_available.notify_all();
        while sess_mgr.waiting_worker_threads < sess_mgr.total_worker_threads {
            sess_mgr = ctx.worker_threads_finished.wait(sess_mgr).unwrap();
        }
        let _ = stdout().lock().flush();
    }
}

impl SessionSetupData {
    pub fn validate_chains(&self) -> Result<(), ChainSetupError> {
        for (chain_id, chain) in self.chains.iter_enumerated() {
            let mut message = "";
            // TODO: maybe insert a nop if the chain is empty?
            if chain.settings.default_batch_size == 0 {
                message = "default batch size cannot be zero";
            } else if chain.settings.stream_buffer_size == 0 {
                message = "stream buffer size cannot be zero";
            }
            if !message.is_empty() {
                return Err(ChainSetupError::new(message, chain_id));
            }
        }
        Ok(())
    }

    pub fn add_op_base_raw(
        &mut self,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data_id: OperatorDataId,
    ) -> OperatorId {
        let op_base = OperatorBase::from_opts_interned(
            op_base_opts_interned,
            chain_id,
            op_data_id,
            offset_in_chain,
            self.chains[chain_id].settings.default_batch_size,
        );
        self.operator_bases.push_get_id(op_base)
    }

    pub fn add_op_data_raw(
        &mut self,
        op_data: OperatorData,
    ) -> OperatorDataId {
        self.operator_data.push_get_id(op_data)
    }

    pub fn setup_for_op_data_id(
        &mut self,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data_id: OperatorDataId,
    ) -> Result<OperatorId, OperatorSetupError> {
        let mut op_data = std::mem::take(&mut self.operator_data[op_data_id]);
        let op_id = op_data.setup(
            self,
            chain_id,
            offset_in_chain,
            op_base_opts_interned,
            op_data_id,
        )?;
        self.operator_data[op_data_id] = op_data;
        Ok(op_id)
    }

    pub fn setup_for_op_data(
        &mut self,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data: OperatorData,
    ) -> Result<OperatorId, OperatorSetupError> {
        let op_data_id = self.operator_data.push_get_id(op_data);
        self.setup_for_op_data_id(
            chain_id,
            offset_in_chain,
            op_base_opts_interned,
            op_data_id,
        )
    }

    pub fn add_op_from_offset_in_chain(
        &mut self,
        chain_id: ChainId,
        offset_in_chain: OperatorOffsetInChain,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data_id: OperatorDataId,
    ) -> OperatorId {
        let op_id = self.add_op_base_raw(
            chain_id,
            offset_in_chain,
            op_base_opts_interned,
            op_data_id,
        );
        match offset_in_chain {
            OperatorOffsetInChain::Direct(offset) => {
                debug_assert_eq!(
                    offset,
                    self.chains[chain_id].operators.next_idx()
                );
                self.chains[chain_id].operators.push(op_id);
            }
            OperatorOffsetInChain::AggregationMember(_, _) => (),
        }
        op_id
    }

    pub fn add_op_from_opts_interned_direct(
        &mut self,
        chain_id: ChainId,
        op_base_opts_interned: OperatorBaseOptionsInterned,
        op_data_id: OperatorDataId,
    ) -> OperatorId {
        debug_assert!(!op_base_opts_interned.append_mode);
        let op_id = self.add_op_base_raw(
            chain_id,
            OperatorOffsetInChain::Direct(
                self.chains[chain_id].operators.next_idx(),
            ),
            op_base_opts_interned,
            op_data_id,
        );
        self.chains[chain_id].operators.push(op_id);
        op_id
    }

    pub fn add_op_from_opts_direct(
        &mut self,
        chain_id: ChainId,
        op_base_opts: OperatorBaseOptions,
        op_data_id: Option<OperatorDataId>,
    ) -> OperatorId {
        let op_data = op_data_id.map(|id| &self.operator_data[id]);
        let op_base_opts_interned =
            op_base_opts.intern(op_data, &mut self.string_store);
        self.add_op_from_opts_interned_direct(
            chain_id,
            op_base_opts_interned,
            op_data_id.unwrap_or(OperatorDataId::MAX_VALUE),
        )
    }

    pub fn add_op_direct(
        &mut self,
        chain_id: ChainId,
        op_base_opts: OperatorBaseOptions,
        op_data: OperatorData,
    ) -> OperatorId {
        let op_data_id = self.operator_data.push_get_id(op_data);
        self.add_op_from_opts_direct(chain_id, op_base_opts, Some(op_data_id))
    }

    pub fn setup_chain(
        &mut self,
        chain_id: ChainId,
        chain_data: impl IntoIterator<
            Item = (OperatorBaseOptionsInterned, OperatorDataId),
        >,
    ) -> Result<ChainId, OperatorSetupError> {
        let mut curr_aggregation_op_id = None;
        let mut curr_aggregation = IndexVec::new();

        let mut iter = chain_data.into_iter().peekable();
        while let Some((op_base_opts, op_data_id)) = iter.next() {
            let successor_append = iter
                .peek()
                .map(|(base_opts, _)| base_opts.append_mode)
                .unwrap_or(false);

            let add_to_aggregation =
                op_base_opts.append_mode || successor_append;

            if add_to_aggregation {
                let curr_agg =
                    *curr_aggregation_op_id.get_or_insert_with(|| {
                        let aggregate_op_opts =
                            OperatorBaseOptions::from_name("aggregate");
                        self.add_op_from_opts_direct(
                            chain_id,
                            aggregate_op_opts,
                            None,
                        )
                    });

                if curr_aggregation.is_empty() && op_base_opts.append_mode {
                    let op_data = create_op_nop_copy();
                    let op_nop_opts = OperatorBaseOptions::from_name("nop")
                        .intern(Some(&op_data), &mut self.string_store);
                    curr_aggregation.push(self.setup_for_op_data(
                        chain_id,
                        OperatorOffsetInChain::AggregationMember(
                            curr_agg,
                            curr_aggregation.next_idx(),
                        ),
                        op_nop_opts,
                        op_data,
                    )?);
                }

                let op_id = self.setup_for_op_data_id(
                    chain_id,
                    OperatorOffsetInChain::AggregationMember(
                        curr_agg,
                        curr_aggregation.next_idx(),
                    ),
                    op_base_opts,
                    op_data_id,
                )?;
                curr_aggregation.push(op_id);
            } else {
                if let Some(curr_agg_id) = curr_aggregation_op_id.take() {
                    let op_data_id =
                        self.add_op_data_raw(create_op_aggregate_raw(
                            std::mem::take(&mut curr_aggregation).into(),
                        ));
                    self.operator_bases[curr_agg_id].op_data_id = op_data_id;
                }
                self.setup_for_op_data_id(
                    chain_id,
                    OperatorOffsetInChain::Direct(
                        self.chains[chain_id].operators.next_idx(),
                    ),
                    op_base_opts,
                    op_data_id,
                )?;
            }
        }

        if let Some(curr_ag) = curr_aggregation_op_id {
            self.operator_bases[curr_ag].op_data_id = self.add_op_data_raw(
                create_op_aggregate_raw(curr_aggregation.into()),
            );
        }

        Ok(chain_id)
    }

    pub fn create_subchain(
        &mut self,
        chain_id: ChainId,
        subchain_data: Vec<(OperatorBaseOptions, OperatorData)>,
    ) -> Result<ChainId, OperatorSetupError> {
        let subchain_id = self.chains.push_get_id(Chain {
            label: None,
            settings: self.chains[chain_id].settings.clone(),
            operators: IndexVec::new(),
            subchains: IndexVec::new(),
            scope_id: self
                .scope_mgr
                .add_scope(Some(self.chains[chain_id].scope_id)),
        });
        self.chains[chain_id].subchains.push(subchain_id);
        // PERF: :/
        let subchain_processed = subchain_data
            .into_iter()
            .map(|(opts, data)| {
                (
                    opts.intern(Some(&data), &mut self.string_store),
                    self.add_op_data_raw(data),
                )
            })
            .collect::<Vec<_>>();
        self.setup_chain(subchain_id, subchain_processed)
    }
}

impl SessionData {
    pub fn with_mut_op_data<R>(
        &mut self,
        op_data_id: OperatorDataId,
        f: impl FnOnce(&mut Self, &mut OperatorData) -> R,
    ) -> R {
        let mut op_data = std::mem::take(&mut self.operator_data[op_data_id]);
        let res = f(self, &mut op_data);
        self.operator_data[op_data_id] = op_data;
        res
    }
    pub fn setup_op_liveness(&mut self, ld: &LivenessData, op_id: OperatorId) {
        let op_data_id = self.op_data_id(op_id);
        self.with_mut_op_data(op_data_id, |sess, op_data| {
            op_data.on_liveness_computed(sess, ld, op_id)
        });
    }
    pub fn compute_liveness(&mut self) {
        let ld = liveness_analysis::compute_liveness_data(self);
        for op_id in
            IndexingTypeRange::from_zero(self.operator_bases.next_idx())
        {
            Self::setup_op_liveness(self, &ld, op_id);
        }
    }

    pub fn has_no_command(&self, opts: &CliOptions) -> bool {
        let op_count = self.chains[ChainId::zero()].operators.len();
        // HACK this sucks. we should probably add some bool like
        // `has_arguments` or some other mechanism that is less hacky
        // instead
        let implicit_op_count = [
            opts.start_with_stdin,
            opts.print_output,
            opts.add_success_updator,
            true, // for the terminator
        ]
        .iter()
        .copied()
        .map(usize::from)
        .sum::<usize>();
        // smaller is possible if this session is the result of an
        // NoArgumentsError
        op_count <= implicit_op_count
    }
    pub fn construct_main_chain_job(
        &self,
        input_data: RecordSet,
    ) -> JobDescription {
        let operator =
            self.chains[ChainId::zero()].operators[OffsetInChain::zero()];
        JobDescription {
            operator,
            data: input_data,
        }
    }
    pub fn op_data_id(&self, op_id: OperatorId) -> OperatorDataId {
        self.operator_bases[op_id].op_data_id
    }
    pub fn run_job_unthreaded(&self, job: JobDescription) {
        assert!(!self.settings.repl);
        let mut js = Job::from_job_data(JobData::new(self));
        js.setup_job(job);
        if let Err(_venture) = js.run(None) {
            unreachable!()
        }
    }
    pub fn run(self, job: JobDescription) {
        assert!(!self.settings.repl);
        if self.settings.max_threads == 1 {
            self.run_job_unthreaded(job);
        } else {
            Context::new(Arc::new(self)).run_job(job);
        }
    }
    pub fn repl_requested(&self) -> bool {
        self.settings.repl
    }
    pub fn set_success(&self, success: bool) {
        self.success
            .store(success, std::sync::atomic::Ordering::SeqCst)
    }
    pub fn get_success(&self) -> bool {
        self.success.load(std::sync::atomic::Ordering::SeqCst)
    }
}
