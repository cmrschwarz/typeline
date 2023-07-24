use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

use bstr::ByteSlice;
use reedline::{
    DefaultPrompt, DefaultPromptSegment, FileBackedHistory, History, HistoryItem, Reedline, Signal,
};
use shlex::Shlex;
use smallvec::SmallVec;

use crate::chain::ChainId;
use crate::cli::parse_cli;
use crate::field_data::record_buffer::RecordBuffer;
use crate::field_data::record_set::RecordSet;
use crate::job_session::{JobData, JobSession};
use crate::operators::operator::OperatorId;
use crate::utils::identity_hasher::BuildIdentityHasher;
use crate::utils::string_store::StringStoreEntry;
use crate::utils::temp_vec::TempVec;
use crate::{
    chain::Chain,
    operators::operator::{OperatorBase, OperatorData},
    scr_error::ScrError,
    utils::string_store::StringStore,
    worker_thread::WorkerThread,
};

pub struct Job {
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
    pub source_job_session: Option<Box<JobSession<'a>>>,
}

pub struct Session {
    pub(crate) max_threads: usize,
    pub(crate) repl: bool,
    pub(crate) chains: Vec<Chain>,
    pub(crate) chain_labels: HashMap<StringStoreEntry, ChainId, BuildIdentityHasher>,
    pub(crate) operator_bases: Vec<OperatorBase>,
    pub(crate) operator_data: Vec<OperatorData>,
    pub(crate) cli_args: Option<Vec<Vec<u8>>>,
    pub(crate) string_store: StringStore,
}

pub(crate) struct SessionManager {
    pub session: Arc<Session>,
    pub job_queue: VecDeque<Job>,
    pub venture_queue: VecDeque<Venture<'static>>,
    pub terminate: bool,
    pub waiting_worker_threads: usize,
    pub total_worker_threads: usize,

    //TODO
    #[allow(dead_code)]
    pub waiting_venture_participants: usize,
    #[allow(dead_code)]
    pub venture_id_counter: usize,
}

pub(crate) struct ContextData {
    pub work_available: Condvar,
    pub worker_threads_finished: Condvar,
    pub sess_mgr: Mutex<SessionManager>,
}

pub struct Context {
    pub(crate) session: Arc<Session>,
    pub(crate) main_thread: WorkerThread,
}

impl SessionManager {
    #[allow(dead_code)] //TODO
    pub fn submit_job(&mut self, ctx_data: &Arc<ContextData>, job: Job) {
        self.job_queue.push_back(job);
        //TODO: better check
        if self.session.max_threads < self.total_worker_threads && self.waiting_worker_threads == 0
        {
            self.add_worker_thread(ctx_data)
        }
    }
    pub fn add_worker_thread(&mut self, ctx_data: &Arc<ContextData>) {
        let ctx_data = ctx_data.clone();
        self.total_worker_threads += 1;
        // this detaches the worker thread, which is fine for our usecase
        std::thread::spawn(move || WorkerThread::new(ctx_data).run());
    }
    pub fn submit_venture<'a>(
        &mut self,
        desc: VentureDescription,
        starting_job_session: Option<Box<JobSession<'a>>>,
    ) {
        assert!(starting_job_session
            .as_ref()
            .map(|js| std::ptr::eq(js.job_data.session_data, self.session.as_ref()))
            .unwrap_or(true));
        let id = self.venture_id_counter;
        self.venture_id_counter = id.wrapping_add(1);
        self.venture_queue.push_back(Venture {
            description: desc,
            venture_id: id,
            source_job_session: unsafe { std::mem::transmute(starting_job_session) },
        });
    }
    pub fn set_session(&mut self, sess: Arc<Session>) {
        assert!(self.job_queue.is_empty());
        assert!(self.venture_queue.is_empty());
        // SAFETY: the asserts above make sure that nobody is still refering to the old session
        self.session = sess;
    }
}

impl Context {
    pub fn new(session: Arc<Session>) -> Self {
        let ctx_data = Arc::new(ContextData {
            work_available: Condvar::new(),
            worker_threads_finished: Condvar::new(),
            sess_mgr: Mutex::new(SessionManager {
                terminate: false,
                total_worker_threads: 0,
                venture_id_counter: 0,
                waiting_venture_participants: 0,
                waiting_worker_threads: 0,
                venture_queue: Default::default(),
                job_queue: Default::default(),
                session: unsafe { std::mem::transmute(session.as_ref()) },
            }),
        });
        Self {
            main_thread: WorkerThread::new(ctx_data),
            session,
        }
    }
    fn wait_for_worker_threads(&self) {
        let ctx = self.main_thread.ctx_data.as_ref();
        let mut sess_mgr = ctx.sess_mgr.lock().unwrap();
        loop {
            if sess_mgr.waiting_worker_threads == sess_mgr.total_worker_threads {
                return;
            }
            sess_mgr = ctx.worker_threads_finished.wait(sess_mgr).unwrap();
        }
    }
    pub fn set_session(&mut self, mut session: Session) {
        if self.session.max_threads < session.max_threads {
            // TODO: we might want to lower ours instead?
            // but this is simple and prevents deadlocks for now
            session.max_threads = self.session.max_threads;
        }
        let new_sess = Arc::new(session);

        // PERF: this lock is completely pointless, we known nobody else
        // is using this right now. we could get rid of this using unsafe,
        // but it's probably not worth it
        let mut sess_mgr = self.main_thread.ctx_data.sess_mgr.lock().unwrap();
        sess_mgr.set_session(new_sess.clone());
        self.session = new_sess;
    }
    pub fn get_session(&self) -> &Session {
        &self.session
    }
    pub fn run_job(&mut self, job: Job) {
        self.main_thread
            .run_job(unsafe { std::mem::transmute(&self.session) }, job);
        if self.session.max_threads > 1 {
            self.wait_for_worker_threads();
        }
    }
    pub fn run_main_chain(&mut self, input_data: RecordSet) {
        self.run_job(self.session.construct_main_chain_job(input_data))
    }
    pub fn run_repl(&mut self) {
        if !self.session.has_no_command() {
            self.run_main_chain(RecordSet::default());
        }
        let mut history = Box::new(FileBackedHistory::default());
        if let Some(args) = &self.session.cli_args {
            history
                .save(HistoryItem::from_command_line(
                    &args
                        .iter()
                        .map(|arg| arg.to_str_lossy())
                        .fold(String::new(), |mut s, a| {
                            s.push_str(" ");
                            s.push_str(&shlex::quote(&a));
                            s
                        })[1..],
                ))
                .unwrap();
        }
        let mut line_editor = Reedline::create().with_history(history);
        let mut prompt = DefaultPrompt::default();
        prompt.right_prompt = DefaultPromptSegment::Empty;
        prompt.left_prompt = DefaultPromptSegment::Basic("scr ".to_string());
        loop {
            let sig = line_editor.read_line(&prompt);
            match sig {
                Ok(Signal::Success(buffer)) => {
                    let mut shlex = Shlex::new(&buffer);
                    let args = shlex.by_ref().map(|s| s.into_bytes()).collect();
                    let mut exit_repl = false;
                    let sess = if !shlex.had_error {
                        let mut sess_opts = parse_cli(args, true);
                        sess_opts = sess_opts.and_then(|mut opts| {
                            exit_repl = opts.repl.get() == Some(false)
                                || opts.exit_repl.get() == Some(true);
                            opts.repl.force_set(true, None);
                            Ok(opts)
                        });
                        match sess_opts.and_then(|opts| opts.build_session()) {
                            Ok(opts) => Ok(opts),
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
                    } else {
                        Err("failed to tokenize command line arguments".to_string())
                    };
                    match sess {
                        Ok(sess) => {
                            self.set_session(sess);
                            if !self.session.has_no_command() {
                                self.run_main_chain(RecordSet::default());
                            }
                        }
                        Err(e) => {
                            println!("\x1b[0;31mError:\x1b[0m {e}");
                            continue;
                        }
                    }
                    if exit_repl {
                        break;
                    }
                }
                Ok(Signal::CtrlC) => {
                    println!("^C");
                    continue;
                }
                Ok(Signal::CtrlD) => {
                    println!("exit");
                    break;
                }
                Err(err) => {
                    println!("IO Error: {}", err.to_string());
                    break;
                }
            }
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if self.session.max_threads <= 1 {
            return;
        }
        let ctx = self.main_thread.ctx_data.as_ref();
        let mut sess_mgr = ctx.sess_mgr.lock().unwrap();
        sess_mgr.terminate = true;
        ctx.work_available.notify_all();
        loop {
            sess_mgr = ctx.worker_threads_finished.wait(sess_mgr).unwrap();
            if sess_mgr.waiting_worker_threads == sess_mgr.total_worker_threads {
                return;
            }
        }
    }
}

impl Session {
    pub fn has_no_command(&self) -> bool {
        self.chains[0].operators.is_empty()
    }
    pub fn construct_main_chain_job(&self, input_data: RecordSet) -> Job {
        let operator = self.chains[0].operators[0];
        Job {
            operator,
            data: input_data,
        }
    }
    pub fn run_job_unthreaded(&self, job: Job) {
        assert!(!self.repl);
        let mut js = JobSession {
            transform_data: Vec::new(),
            job_data: JobData::new(&self),
            temp_vec: TempVec::default(),
        };
        js.setup_job(job);
        if let Err(_venture) = js.run(None) {
            unreachable!()
        }
    }
    pub fn run(self, job: Job) {
        assert!(!self.repl);
        if self.max_threads == 1 {
            self.run_job_unthreaded(job);
        } else {
            Context::new(Arc::new(self)).run_job(job);
        }
    }
    pub fn repl_requested(&self) -> bool {
        self.repl
    }
}
