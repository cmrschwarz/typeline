use std::borrow::Borrow;
use std::collections::VecDeque;
use std::iter;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crossbeam::deque::{Injector, Stealer, Worker};
use smallvec::SmallVec;

use crate::chain::Chain;
use crate::document::{Document, DocumentSource};
use crate::operations::read_stdin::TfReadStdin;
use crate::operations::{Operation, OperationBase, OperationRef};
use crate::options;
use crate::transform::Transform;

pub struct Job {
    ops: SmallVec<[OperationRef; 2]>,
    tf: Box<dyn Transform>,
}

pub struct ContextData {
    pub parallel_jobs: usize,
    pub documents: Vec<Document>,
    pub chains: Vec<Chain>,
    pub operations: Vec<Box<dyn Operation>>,
}
pub struct SessionData {
    generation: usize,
    terminate: bool,
    stealers: Vec<Stealer<Job>>,
    ctx_data: Arc<ContextData>,
}

pub struct Session {
    pub injector: Injector<Job>,
    pub tasks_available: Condvar,
    pub session_data: Mutex<SessionData>,
}

pub struct Context {
    pub data: Arc<ContextData>,
    pub session: Arc<Session>,
    pub worker_join_handles: Vec<std::thread::JoinHandle<()>>,
}

impl Context {
    pub fn new(data: ContextData) -> Self {
        let data_arc = Arc::new(data);
        Self {
            data: data_arc.clone(),
            session: Arc::new(Session {
                injector: Injector::new(),
                tasks_available: Condvar::new(),
                session_data: Mutex::new(SessionData {
                    generation: 0,
                    terminate: false,
                    stealers: Vec::new(),
                    ctx_data: data_arc,
                }),
            }),
            worker_join_handles: Default::default(),
        }
    }
}

struct WorkerThread<'a> {
    worker: Worker<Job>,
    session: &'a Session,
    ctx: Arc<ContextData>,
    stealers: Vec<Stealer<Job>>,
    session_generation: usize,
}

impl WorkerThread<'_> {
    fn run(&mut self) {
        loop {
            if let Some(job) = self.find_job() {
                self.run_job(job);
            } else {
                if !self.get_next_session() {
                    return;
                }
            }
        }
    }
    fn get_next_session(&mut self) -> bool {
        loop {
            let session_data = self
                .session
                .tasks_available
                .wait(self.session.session_data.lock().unwrap())
                .unwrap();
            if session_data.generation != self.session_generation {
                if session_data.terminate {
                    return false;
                }
                self.update_session(&session_data);
                return true;
            }
        }
    }
    fn update_session(&mut self, sess: &SessionData) {
        self.session_generation = sess.generation;
        self.stealers.extend(
            sess.stealers
                .iter()
                .skip(self.stealers.len())
                .map(|s| s.clone()),
        );
        self.ctx = sess.ctx_data.clone();
    }
    fn find_job(&mut self) -> Option<Job> {
        self.worker.pop().or_else(|| {
            iter::repeat_with(|| {
                self.session
                    .injector
                    .steal_batch_and_pop(&self.worker)
                    .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
            })
            .find(|s| !s.is_retry())
            .and_then(|s| s.success())
        })
    }

    fn run_job(&mut self, job: Job) {}
}

impl Context {
    pub fn gen_jobs_from_docs(&mut self) {
        let cd = self.data.as_ref();
        let mut stdin_job_ops: SmallVec<[OperationRef; 2]> = Default::default();
        for d in &cd.documents {
            let mut ops_iter = d.target_chains.iter().map(|c| OperationRef::new(*c, 0));
            match d.source {
                DocumentSource::Stdin => {
                    stdin_job_ops.extend(ops_iter);
                }
                _ => {
                    self.session.injector.push(Job {
                        ops: ops_iter.collect(),
                        tf: d.source.create_start_transform(),
                    });
                }
            }
        }
        if !stdin_job_ops.is_empty() {
            self.session.injector.push(Job {
                tf: Box::new(TfReadStdin::new()),
                ops: stdin_job_ops,
            });
        }
    }
    pub fn run(&mut self) {
        self.gen_jobs_from_docs();
        let additional_wts = self.data.parallel_jobs - self.worker_join_handles.len();
        let workers = (0..additional_wts)
            .map(|_| Worker::new_fifo())
            .collect::<Vec<Worker<Job>>>();
        self.session
            .session_data
            .lock()
            .unwrap()
            .stealers
            .extend(workers.iter().map(|w| w.stealer()));
        for worker in workers.into_iter() {
            let ctx_data = self.data.clone();
            let session = self.session.clone();
            self.worker_join_handles.push(std::thread::spawn(move || {
                let sess = session.session_data.lock().unwrap();
                let mut wt = WorkerThread {
                    worker,
                    session: session.as_ref(),
                    ctx: ctx_data.clone(),
                    stealers: sess.stealers.clone(),
                    session_generation: sess.generation,
                };
                wt.run();
            }));
        }
    }
}
