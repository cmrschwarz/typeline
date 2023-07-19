use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use smallvec::{smallvec, SmallVec};

use crate::field_data::record_set::RecordSet;
use crate::operations::operator::OperatorId;
use crate::worker_thread_session::{JobSession, WorkerThreadSession};
use crate::{
    chain::Chain,
    operations::operator::{OperatorBase, OperatorData},
    scr_error::ScrError,
    utils::string_store::StringStore,
    worker_thread::WorkerThread,
};

pub struct Job {
    pub starting_ops: SmallVec<[OperatorId; 2]>,
    pub data: RecordSet,
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

pub(crate) struct SessionManager {
    pub job_queue: VecDeque<Job>,
    pub terminate: bool,
    pub waiting_worker_threads: usize,
    pub total_worker_threads: usize,
    pub session: Arc<Session>,
    pub worker_join_handles: Vec<std::thread::JoinHandle<Result<(), ScrError>>>,
}

pub(crate) struct ContextData {
    pub jobs_available: Condvar,
    pub worker_threads_finished: Condvar,
    pub sess_mgr: Mutex<SessionManager>,
}

pub struct Context {
    pub(crate) session: Arc<Session>,
    pub(crate) main_worker_thread: WorkerThread,
}

impl Context {
    pub fn new(session: Arc<Session>) -> Self {
        let ctx_data = Arc::new(ContextData {
            jobs_available: Condvar::new(),
            worker_threads_finished: Condvar::new(),
            sess_mgr: Mutex::new(SessionManager {
                terminate: false,
                session: session.clone(),
                total_worker_threads: 0,
                waiting_worker_threads: 0,
                worker_join_handles: Default::default(),
                job_queue: Default::default(),
            }),
        });
        Self {
            main_worker_thread: WorkerThread::new(ctx_data, session.clone()),
            session: session,
        }
    }
    fn wait_for_worker_threads(&self) {
        let ctx = self.main_worker_thread.ctx_data.as_ref();
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
        self.session = Arc::new(session);
        self.main_worker_thread.update_session(self.session.clone());

        // PERF: this lock is completely pointless, we known nobody else
        // is using this right now. we could get rid of this using unsafe,
        // but it's probably not worth it
        let mut sess_mgr = self.main_worker_thread.ctx_data.sess_mgr.lock().unwrap();
        sess_mgr.session = self.session.clone();
    }
    pub fn get_session(&self) -> &Session {
        &self.session
    }
    pub fn run_job(&mut self, job: Job) {
        self.main_worker_thread.run_job(job);
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
        let ctx = self.main_worker_thread.ctx_data.as_ref();
        let mut sess_mgr = ctx.sess_mgr.lock().unwrap();
        sess_mgr.terminate = true;
        ctx.jobs_available.notify_all();
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
            starting_ops: smallvec![starting_op],
            data: input_data,
        }
    }
    pub fn run_job_unthreaded(&self, job: Job) {
        let mut transform_data = Vec::new();
        let mut job_session = JobSession::new(&self);
        let mut wts = WorkerThreadSession {
            transform_data: &mut transform_data,
            job_session: &mut job_session,
        };
        wts.run_job(job)
    }

    pub fn run(self, job: Job) {
        if self.max_threads == 1 {
            self.run_job_unthreaded(job)
        } else {
            Context::new(Arc::new(self)).run_job(job)
        }
    }
}
