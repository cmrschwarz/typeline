use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};

use crossbeam::deque::{Injector, Stealer, Worker};
use smallvec::SmallVec;

use crate::chain::Chain;
use crate::document::{Document, DocumentSource};
use crate::operations::operation::{Operation, OperationRef};
use crate::scr_error::ScrError;
use crate::worker_thread::{Job, WorkerThread};

pub struct SessionData {
    pub max_worker_threads: NonZeroUsize,
    pub is_repl: bool,
    pub documents: Vec<Document>,
    pub chains: Vec<Chain>,
    pub operations: Vec<Box<dyn Operation>>,
}

pub(crate) struct Session {
    pub(crate) generation: usize,
    pub(crate) terminate: bool,
    pub(crate) stealers: Vec<Stealer<Job>>,
    pub(crate) data: Arc<SessionData>,
}

// shared between worker threads using an Arc<ContextData>
pub(crate) struct ContextData {
    pub(crate) injector: Injector<Job>,
    pub(crate) tasks_available: Condvar,
    pub(crate) session: Mutex<Session>,
}

pub struct Context {
    // we need pub(crate) to contextualize error messages for ScrError
    pub(crate) curr_session_data: Arc<SessionData>,
    main_worker_thread: WorkerThread,
    worker_join_handles: Vec<std::thread::JoinHandle<Result<(), ScrError>>>,
}

impl Context {
    pub fn new(session_data: SessionData) -> Self {
        let session_data_arc = Arc::new(session_data);
        let ctx_data_arc = Arc::new(ContextData {
            injector: Injector::new(),
            tasks_available: Condvar::new(),
            session: Mutex::new(Session {
                generation: 0,
                terminate: false,
                stealers: Vec::new(),
                data: session_data_arc.clone(),
            }),
        });

        Self {
            curr_session_data: session_data_arc,
            main_worker_thread: WorkerThread::new(0, Worker::new_fifo(), ctx_data_arc.clone()),
            worker_join_handles: Default::default(),
        }
    }
    pub fn gen_jobs_from_docs(&mut self) {
        let sd = self.curr_session_data.as_ref();
        let mut stdin_job_ops: SmallVec<[OperationRef; 2]> = Default::default();
        for d in &sd.documents {
            let ops_iter = d.target_chains.iter().map(|c| OperationRef::new(*c, 0));
            match d.source {
                DocumentSource::Stdin => {
                    stdin_job_ops.extend(ops_iter);
                }
                _ => {
                    self.main_worker_thread.context_data.injector.push(Job {
                        ops: ops_iter.collect(),
                        data: Some(d.source.create_match_data()),
                        args: HashMap::default(),
                        is_stdin: false,
                    });
                }
            }
        }
        if !stdin_job_ops.is_empty() {
            self.main_worker_thread.context_data.injector.push(Job {
                ops: stdin_job_ops,
                data: None,
                args: HashMap::default(),
                is_stdin: true,
            });
        }
        self.main_worker_thread
            .context_data
            .tasks_available
            .notify_all();
    }
    pub fn perform_jobs(&mut self) -> Result<(), ScrError> {
        self.gen_jobs_from_docs();
        assert!(self.curr_session_data.max_worker_threads.get() > self.worker_join_handles.len()); // TODO: handle this case
        let additional_worker_count =
            self.curr_session_data.max_worker_threads.get() - self.worker_join_handles.len() - 1;
        let additional_workers = (0..additional_worker_count)
            .map(|_| Worker::new_fifo())
            .collect::<Vec<Worker<Job>>>();
        {
            let mut session = self.main_worker_thread.context_data.session.lock().unwrap();
            session.generation += 1;
            session
                .stealers
                .extend(additional_workers.iter().map(|w| w.stealer()));
        }
        let mut index = self.worker_join_handles.len() + 1;
        let is_repl = self.curr_session_data.is_repl;
        for worker in additional_workers.into_iter() {
            let session = self.main_worker_thread.context_data.clone();
            self.worker_join_handles.push(std::thread::spawn(move || {
                WorkerThread::new(index, worker, session).run(is_repl)
            }));
            index += 1;
        }
        self.main_worker_thread.run(false)?;
        Ok(()) //TODO
    }
    pub fn terminate(mut self) {
        {
            let mut sess = self.main_worker_thread.context_data.session.lock().unwrap();
            sess.generation += 1;
            sess.terminate = true;
            self.main_worker_thread
                .context_data
                .tasks_available
                .notify_all();
        }
        let threads = std::mem::replace(&mut self.worker_join_handles, Default::default());
        for wt in threads.into_iter() {
            //TODO: bundle up these errors somehow
            wt.join().unwrap().unwrap();
        }
    }
    pub fn run(mut self) -> Result<(), ScrError> {
        self.perform_jobs()?;
        self.terminate();
        Ok(())
    }
}
