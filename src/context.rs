use std::collections::{HashMap, VecDeque};
use std::iter;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};

use crossbeam::deque::{Injector, Stealer, Worker};
use smallvec::SmallVec;

use crate::chain::Chain;
use crate::document::{Document, DocumentSource};
use crate::operations::parent::TfParent;
use crate::operations::read_stdin::TfReadStdin;
use crate::operations::start::TfStart;
use crate::operations::transform::{
    DataKind, MatchData, MatchIdx, TfBase, Transform, TransformOutput, TransformStackIndex,
};
use crate::operations::{Operation, OperationRef};
use crate::scr_error::ScrError;

pub struct Job {
    ops: SmallVec<[OperationRef; 2]>,
    data: Option<MatchData>,
    args: HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
    is_stdin: bool,
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
    pub worker_join_handles: Vec<std::thread::JoinHandle<Result<(), ScrError>>>,
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
            let ops_iter = d.target_chains.iter().map(|c| OperationRef::new(*c, 0));
            match d.source {
                DocumentSource::Stdin => {
                    stdin_job_ops.extend(ops_iter);
                }
                _ => {
                    self.session.injector.push(Job {
                        ops: ops_iter.collect(),
                        data: Some(d.source.create_match_data()),
                        args: HashMap::default(),
                        is_stdin: false,
                    });
                }
            }
        }
        if !stdin_job_ops.is_empty() {
            self.session.injector.push(Job {
                ops: stdin_job_ops,
                data: None,
                args: HashMap::default(),
                is_stdin: true,
            });
        }
    }
    pub fn run(&mut self) -> Result<(), ScrError> {
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
            let session = self.session.clone();
            self.worker_join_handles.push(std::thread::spawn(move || {
                let sd = session.session_data.lock().unwrap();
                let mut wt = WorkerThread::new(index, worker, session.as_ref(), &sd);
                wt.run()
            }));
            index += 1;
        }
        Ok(()) //TODO
    }
}
struct MatchContext {
    ref_count: u32,
    args: HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
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
                .filter(|(idx, _)| *idx != index)
                .map(|(_, s)| s.clone())
                .collect(),
            session_generation: sd.generation,
        }
    }
    fn run(&mut self) -> Result<(), ScrError> {
        loop {
            if let Some(job) = self.find_job() {
                self.run_job(job)?;
            } else {
                if !self.get_next_session() {
                    return Ok(());
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
    fn setup_transform_stack(
        &self,
        job: &Job,
    ) -> Result<SmallVec<[Box<dyn Transform>; 4]>, ScrError> {
        let mut tf_stack: SmallVec<[Box<dyn Transform>; 4]> = Default::default();
        if job.is_stdin {
            tf_stack.push(Box::new(TfReadStdin::new()))
        } else {
            tf_stack.push(Box::new(TfStart::new(
                job.data.as_ref().map_or(DataKind::None, |d| d.kind()),
            )))
        }
        for (i, op_ref) in job.ops.iter().rev().enumerate() {
            let cn = &self.ctx.chains[op_ref.chain_id as usize];
            for i in op_ref.op_offset as usize..cn.operations.len() {
                let op = &self.ctx.operations[cn.operations[i] as usize];
                let tf = op.apply(*op_ref, tf_stack.as_mut_slice())?;
                let requires_eval = tf.requires_eval;
                tf_stack.push(tf);
                if requires_eval {
                    break;
                }
            }
            if i + 1 < job.ops.len() {
                let mut tf_base = TfBase::from_parent(&tf_stack[0]);
                tf_base.begin_of_chain = true;
                tf_base.tfs_index = tf_stack.len() as TransformStackIndex;
                tf_stack.push(Box::new(TfParent {
                    tf_base,
                    op_ref: OperationRef {
                        //this little lie is fine since we will never error on this one anyways
                        chain_id: 0,
                        op_offset: 0,
                    },
                    offset: tf_stack.len() as TransformStackIndex,
                }));
            }
        }
        Ok(tf_stack)
    }
    fn run_job(&mut self, job: Job) -> Result<(), ScrError> {
        let mut tf_stack = self.setup_transform_stack(&job)?;
        let mut matches_ctx: VecDeque<MatchContext> = Default::default();
        matches_ctx.push_back(MatchContext {
            ref_count: 1,
            args: job.args,
        });

        let mut outputs: Vec<VecDeque<(usize, TransformOutput)>> = Vec::new();
        outputs.resize_with(tf_stack.len(), Default::default);
        outputs[0].push_back((
            0,
            TransformOutput {
                match_index: 0 as MatchIdx,
                data: job.data,
                args: Vec::default(),
                is_last_chunk: None,
            },
        ));
        let mut finished_matches: usize = 0;
        let mut last_tf_with_pending_output = 0;
        loop {
            let tf_idx = last_tf_with_pending_output;
            if let Some(tfo) = outputs[last_tf_with_pending_output].front_mut() {
                let match_idx = tfo.1.match_index as usize;
                if tfo.0 >= tf_stack[tf_idx].dependants.len() {
                    let rc = &mut matches_ctx[match_idx].ref_count;
                    *rc -= 1;
                    if *rc == 0 {
                        matches_ctx[match_idx].args.clear();
                        if match_idx != finished_matches + 1 {
                            matches_ctx[match_idx].args = Default::default();
                        } else {
                            while matches_ctx[0].ref_count == 0 {
                                matches_ctx.pop_front();
                                finished_matches += 1;
                            }
                        }
                    }
                    outputs[last_tf_with_pending_output].pop_front();
                    continue;
                }
                let dep_idx = tf_stack[tf_idx].dependants[tfo.0] as usize;
                tfo.0 += 1;
                let res = tf_stack[dep_idx].process(
                    &self.ctx,
                    &matches_ctx[match_idx - finished_matches].args,
                    &tfo.1,
                )?;
                for res_tfo in &res {
                    matches_ctx[res_tfo.match_index as usize - finished_matches].ref_count += 1;
                }
                outputs[dep_idx].extend(res.into_iter().map(|tfo| (0, tfo)));
                if dep_idx > last_tf_with_pending_output {
                    last_tf_with_pending_output = dep_idx;
                }
            } else {
                if last_tf_with_pending_output == 0 {
                    break;
                }
                last_tf_with_pending_output -= 1;
            }
        }

        Ok(())
    }
}
