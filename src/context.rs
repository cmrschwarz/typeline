use std::borrow::Borrow;
use std::collections::VecDeque;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use crossbeam::deque::{Injector, Stealer, Worker};
use smallvec::SmallVec;

use crate::chain::Chain;
use crate::document::{Document, DocumentSource};
use crate::operations::read_stdin::TfReadStdin;
use crate::operations::{OpBase, Operation, OperationRef};
use crate::options;
use crate::transform::Transform;

pub struct Job {
    ops: SmallVec<[OperationRef; 2]>,
    tf: Box<dyn Transform>,
}

pub struct ContextData {
    pub parallel_jobs: NonZeroUsize,
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
    pub main_thread_worker: Worker<Job>,
    pub worker_join_handles: Vec<std::thread::JoinHandle<()>>,
}

struct WorkerThread<'a> {
    worker: Worker<Job>,
    session: &'a Session,
    ctx: Arc<ContextData>,
    stealers: Vec<Stealer<Job>>,
    session_generation: usize,
}

impl Context {
    pub fn new(data: ContextData) -> Self {
        let data_arc = Arc::new(data);
        let session_arc = Arc::new(Session {
            injector: Injector::new(),
            tasks_available: Condvar::new(),
            session_data: Mutex::new(SessionData {
                generation: 0,
                terminate: false,
                stealers: Vec::new(),
                ctx_data: data_arc.clone(),
            }),
        });
        Self {
            data: data_arc,
            session: session_arc,
            main_thread_worker: Worker::new_fifo(),
            worker_join_handles: Default::default(),
        }
    }
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
        assert!(self.data.parallel_jobs.get() > self.worker_join_handles.len()); // TODO: handle this case
        let additional_wts = self.data.parallel_jobs.get() - self.worker_join_handles.len() - 1;
        let workers = (0..additional_wts)
            .map(|_| Worker::new_fifo())
            .collect::<Vec<Worker<Job>>>();
        self.session
            .session_data
            .lock()
            .unwrap()
            .stealers
            .extend(workers.iter().map(|w| w.stealer()));
        let mut index = self.worker_join_handles.len() + 1;
        for worker in workers.into_iter() {
            let ctx_data = self.data.clone();
            let session = self.session.clone();
            self.worker_join_handles.push(std::thread::spawn(move || {
                let sd = session.session_data.lock().unwrap();
                let mut wt = WorkerThread::new(index, worker, session.as_ref(), &sd);
                wt.run();
            }));
            index += 1;
        }
    }
}

impl<'a> WorkerThread<'a> {
    fn new(index: usize, worker: Worker<Job>, session: &'a Session, sd: &SessionData) -> Self {
        Self {
            worker: worker,
            session: session,
            ctx: sd.ctx_data.clone(),
            stealers: sd
                .stealers
                .iter()
                .enumerate()
                .filter(|(idx, s)| *idx != index)
                .map(|(idx, s)| s.clone())
                .collect(),
            session_generation: sd.generation,
        }
    }
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
