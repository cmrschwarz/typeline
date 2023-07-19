use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use smallvec::SmallVec;

use crate::field_data::record_set::RecordSet;
use crate::job_session::{JobData, JobSession};
use crate::operations::operator::OperatorId;
use crate::{
    chain::Chain,
    operations::operator::{OperatorBase, OperatorData},
    scr_error::ScrError,
    utils::string_store::StringStore,
    worker_thread::WorkerThread,
};

pub struct Job {
    pub starting_op: OperatorId,
    pub data: RecordSet,
}

pub struct VentureDescription {
    pub participans_needed: usize,
    pub starting_points: SmallVec<[OperatorId; 4]>,
}

pub struct Venture<'a> {
    pub description: VentureDescription,
    pub base_session: Option<Arc<JobSession<'a>>>,
}

pub struct Session {
    pub(crate) max_threads: usize,
    pub(crate) is_repl: bool,
    pub(crate) chains: Vec<Chain>,
    pub(crate) operator_bases: Vec<OperatorBase>,
    pub(crate) operator_data: Vec<OperatorData>,
    pub(crate) cli_args: Option<Vec<Vec<u8>>>,
    pub(crate) string_store: StringStore,
}

pub(crate) struct SessionManager<'a> {
    pub session: &'a Session,
    pub job_queue: VecDeque<Job>,
    pub venture_queue: VecDeque<Venture<'a>>,
    pub waiting_venture_participants: usize,
    pub venture_counter: usize,
    pub terminate: bool,
    pub waiting_worker_threads: usize,
    pub total_worker_threads: usize,
    pub worker_join_handles: Vec<std::thread::JoinHandle<Result<(), ScrError>>>,
}

pub(crate) struct ContextData<'a> {
    pub work_available: Condvar,
    pub worker_threads_finished: Condvar,
    pub sess_mgr: Mutex<SessionManager<'a>>,
}

pub struct Context {
    pub(crate) session: Arc<Session>,
    pub(crate) session_ref: &'static Session,
    pub(crate) main_thread: WorkerThread<'static>,
}

impl Context {
    pub fn new(session: Arc<Session>) -> Self {
        // SAFETY: this type makes sure that all references to this session will be dropped
        // before it drops it itself, so this is fine
        // see set_session for the reassignment process
        let session_ref =
            unsafe { std::mem::transmute::<&'_ Session, &'static Session>(session.as_ref()) };
        let ctx_data = Arc::new(ContextData {
            work_available: Condvar::new(),
            worker_threads_finished: Condvar::new(),
            sess_mgr: Mutex::new(SessionManager {
                terminate: false,
                session: session_ref,
                total_worker_threads: 0,
                venture_counter: 0,
                waiting_venture_participants: 0,
                waiting_worker_threads: 0,
                worker_join_handles: Default::default(),
                venture_queue: Default::default(),
                job_queue: Default::default(),
            }),
        });
        Self {
            main_thread: WorkerThread::new(ctx_data),
            session,
            session_ref,
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
        //for sanity, we check that nobody refers back to the old session
        assert!(sess_mgr.job_queue.is_empty());
        assert!(sess_mgr.venture_queue.is_empty());
        // SAFETY: now that we know that nobody still refers to the old session,
        // it is safe to insert the new one and drop the reference to the old one
        self.session_ref =
            unsafe { std::mem::transmute::<&'_ Session, &'static Session>(new_sess.as_ref()) };
        sess_mgr.session = self.session_ref;
        self.session = new_sess;
    }
    pub fn get_session(&self) -> &Session {
        &self.session
    }
    pub fn run_job(&mut self, job: Job) {
        self.main_thread.run_job(&self.session_ref, job);
        if self.session.max_threads > 1 {
            self.wait_for_worker_threads();
        }
    }
    pub fn run_main_chain(&mut self, input_data: RecordSet) {
        self.run_job(self.session.construct_main_chain_job(input_data))
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
    pub fn construct_main_chain_job(&self, input_data: RecordSet) -> Job {
        let starting_op = self.chains[0].operations[0];
        Job {
            starting_op,
            data: input_data,
        }
    }
    pub fn run_job_unthreaded(&self, job: Job) {
        let mut js = JobSession {
            transform_data: Vec::new(),
            job_data: JobData::new(&self),
        };
        if let Err(_venture_desc) = js.run_job(job, None) {
            unreachable!()
        }
    }

    pub fn run(self, job: Job) {
        if self.max_threads == 1 {
            self.run_job_unthreaded(job)
        } else {
            Context::new(Arc::new(self)).run_job(job)
        }
    }
}
