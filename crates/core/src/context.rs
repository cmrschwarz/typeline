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
    cli::call_expr::CliArgIdx,
    extension::ExtensionRegistry,
    job::{Job, JobData},
    liveness_analysis::{self, LivenessData},
    operators::{
        nop::OpNop,
        operator::{
            OffsetInChain, Operator, OperatorBase, OperatorDataId, OperatorId,
        },
    },
    options::{
        chain_settings::chain_settings_list,
        session_setup::{SessionSetupData, SessionSetupOptions},
    },
    record_data::{
        field_data::FieldData, record_buffer::RecordBuffer,
        record_set::RecordSet, scope_manager::ScopeManager,
    },
    typeline_error::ContextualizedTypelineError,
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

pub struct SessionSettings {
    pub max_threads: usize,
    pub repl: bool,
    pub skipped_first_cli_arg: bool,
    pub debug_log_path: Option<PathBuf>,
    pub debug_log_no_apply: bool,
    pub debug_log_step_min: usize,
    pub debug_break_on_step: Option<usize>,
    pub action_list_cleanup_frequency: usize,
    pub last_cli_output: Option<Arc<FieldData>>,
    pub chain_setting_names: [StringStoreEntry; chain_settings_list::COUNT],
}

pub struct SessionData {
    pub settings: SessionSettings,
    pub scope_mgr: ScopeManager,
    pub chains: IndexVec<ChainId, Chain>,
    pub chain_labels: HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    pub operator_bases: IndexVec<OperatorId, OperatorBase>,
    pub operator_data: IndexVec<OperatorDataId, Box<dyn Operator>>,
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

pub struct Context {
    session: Arc<SessionData>,
    main_thread: WorkerThread,
}

impl SessionData {
    pub fn op_data_id(&self, op_id: OperatorId) -> OperatorDataId {
        self.operator_bases[op_id].op_data_id
    }
    pub fn with_mut_op_data<R>(
        &mut self,
        op_id: OperatorId,
        f: impl FnOnce(&mut Self, &mut Box<dyn Operator>) -> R,
    ) -> R {
        let op_data_id = self.op_data_id(op_id);
        let mut op_data = std::mem::replace(
            &mut self.operator_data[op_data_id],
            Box::new(OpNop::default()),
        );
        let res = f(self, &mut op_data);
        self.operator_data[op_data_id] = op_data;
        res
    }
    pub fn setup_op_liveness(&mut self, ld: &LivenessData, op_id: OperatorId) {
        self.with_mut_op_data(op_id, |sess, op_data| {
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

    pub fn has_no_command(&self) -> bool {
        let op_count = self.chains[ChainId::zero()].operators.len();
        // HACK this sucks. we should probably add some bool like
        // `has_arguments` or some other mechanism that is less hacky
        // instead

        // smaller is possible if this session is the result of an
        // NoArgumentsError
        op_count <= 1
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
    pub fn run_repl(&mut self, mut setup_opts: SessionSetupOptions) {
        use crate::{repl_prompt::ScrPrompt, typeline_error::TypelineError};
        debug_assert!(setup_opts.allow_repl);
        if !self.session.has_no_command() {
            self.run_main_chain(RecordSet::default());
            setup_opts.last_cli_output = setup_opts
                .output_storage
                .as_mut()
                .map(|v| Arc::new(v.get().take_rle()));
        }
        setup_opts.skip_first_cli_arg = false;
        setup_opts.start_with_stdin = false;
        setup_opts.add_success_updator = false;
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
                    let args = shlex
                        .by_ref()
                        .map(String::into_bytes)
                        .collect::<Vec<_>>();
                    let mut exit_repl = false;
                    let sess = if shlex.had_error {
                        Err("failed to tokenize command line arguments"
                            .to_string())
                    } else {
                        match build_repl_session(
                            &setup_opts,
                            args,
                            &mut exit_repl,
                        ) {
                            Ok(sess) => Ok(sess),
                            Err(e) => match e.err {
                                TypelineError::PrintInfoAndExitError(e) => {
                                    println!("{}", e.get_message());
                                    continue;
                                }
                                TypelineError::MissingArgumentsError(_) => {
                                    continue;
                                }
                                _ => Err(e.contextualized_message),
                            },
                        }
                    };
                    match sess {
                        Ok(sess) => {
                            self.set_session(sess);
                            if !self.session.has_no_command() {
                                self.run_main_chain(RecordSet::default());
                                setup_opts.last_cli_output = setup_opts
                                    .output_storage
                                    .as_mut()
                                    .map(|v| Arc::new(v.get().take_rle()));
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

pub fn build_repl_session(
    setup_opts: &SessionSetupOptions,
    args: Vec<Vec<u8>>,
    exit_repl: &mut bool,
) -> Result<SessionData, ContextualizedTypelineError> {
    let mut sess = SessionSetupData::new(setup_opts.clone());

    sess.process_cli_args(args)
        .map_err(|e| sess.contextualize_error(e))?;

    *exit_repl = sess.setup_settings.repl == Some(false)
        || sess.setup_settings.exit_repl == Some(true);

    sess.setup_settings.repl = Some(true);
    sess.build_session_take()
        .map_err(|e| sess.contextualize_error(e))
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
