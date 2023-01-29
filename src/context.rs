use std::collections::{HashMap, VecDeque};
use std::iter;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};

use crossbeam::deque::{Injector, Stealer, Worker};
use smallvec::SmallVec;

use crate::chain::Chain;
use crate::document::{Document, DocumentSource};
use crate::match_data::{MatchData, MatchDataKind};
use crate::operations::operation::{Operation, OperationRef};
use crate::operations::parent::TfParent;
use crate::operations::read_stdin::TfReadStdin;
use crate::operations::start::TfStart;
use crate::operations::transform::{
    MatchIdx, TfBase, Transform, TransformOutput, TransformStackIndex,
};
use crate::scr_error::ScrError;

pub struct Job {
    ops: SmallVec<[OperationRef; 2]>,
    data: Option<MatchData>,
    args: HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
    is_stdin: bool,
}

pub struct ContextData {
    pub parallel_jobs: NonZeroUsize,
    pub is_repl: bool,
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
    main_worker_thread: WorkerThread,
    worker_join_handles: Vec<std::thread::JoinHandle<Result<(), ScrError>>>,
}

#[derive(Default)]
struct MatchContext {
    ref_count: u32,
    args: HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
}
#[derive(Default)]
struct TransformOutputsContext {
    tfos: VecDeque<TransformOutput>,
    next_dependant_indices: VecDeque<usize>,
}

impl TransformOutputsContext {
    fn front_mut(&mut self) -> Option<(&mut TransformOutput, &mut usize)> {
        if let Some(tfo) = self.tfos.front_mut() {
            if let Some(ndi) = self.next_dependant_indices.front_mut() {
                return Some((tfo, ndi));
            }
        }
        None
    }
}

struct WorkerThread {
    worker: Worker<Job>,
    session: Arc<Session>,
    ctx: Arc<ContextData>,
    stealers: Vec<Stealer<Job>>,
    session_generation: usize,

    tf_stack: Vec<Box<dyn Transform>>,
    tf_matches: Vec<MatchContext>,
    tf_outputs: Vec<TransformOutputsContext>,
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
            main_worker_thread: WorkerThread::new(0, Worker::new_fifo(), session_arc.clone()),
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
                    self.main_worker_thread.session.injector.push(Job {
                        ops: ops_iter.collect(),
                        data: Some(d.source.create_match_data()),
                        args: HashMap::default(),
                        is_stdin: false,
                    });
                }
            }
        }
        if !stdin_job_ops.is_empty() {
            self.main_worker_thread.session.injector.push(Job {
                ops: stdin_job_ops,
                data: None,
                args: HashMap::default(),
                is_stdin: true,
            });
        }
    }
    pub fn perform_jobs(&mut self) -> Result<(), ScrError> {
        self.gen_jobs_from_docs();
        assert!(self.data.parallel_jobs.get() > self.worker_join_handles.len()); // TODO: handle this case
        let additional_wts = self.data.parallel_jobs.get() - self.worker_join_handles.len() - 1;
        let workers = (0..additional_wts)
            .map(|_| Worker::new_fifo())
            .collect::<Vec<Worker<Job>>>();
        self.main_worker_thread
            .session
            .session_data
            .lock()
            .unwrap()
            .stealers
            .extend(workers.iter().map(|w| w.stealer()));
        let mut index = self.worker_join_handles.len() + 1;
        let is_repl = self.data.is_repl;
        for worker in workers.into_iter() {
            let session = self.main_worker_thread.session.clone();
            self.worker_join_handles.push(std::thread::spawn(move || {
                WorkerThread::new(index, worker, session).run(is_repl)
            }));
            index += 1;
        }
        self.main_worker_thread.run(false)?;
        Ok(()) //TODO
    }
    pub fn terminate(&mut self) -> Result<(), ScrError> {
        {
            let mut sd = self.main_worker_thread.session.session_data.lock().unwrap();
            sd.generation += 1;
            sd.terminate = true;
            self.main_worker_thread.session.tasks_available.notify_all();
        }
        let threads = std::mem::replace(&mut self.worker_join_handles, Default::default());
        for wt in threads.into_iter() {
            //TODO: bundle up these errors
            wt.join().unwrap().unwrap();
        }
        Ok(())
    }
    pub fn run(&mut self) -> Result<(), ScrError> {
        self.perform_jobs()?;
        self.terminate()?;
        Ok(())
    }
}
impl WorkerThread {
    fn new(index: usize, worker: Worker<Job>, session: Arc<Session>) -> Self {
        let sd = session.session_data.lock().unwrap();
        Self {
            worker: worker,
            ctx: sd.ctx_data.clone(),
            stealers: sd
                .stealers
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != index)
                .map(|(_, s)| s.clone())
                .collect(),
            session_generation: sd.generation,
            session: session.clone(),
            tf_outputs: Default::default(),
            tf_stack: Default::default(),
            tf_matches: Default::default(),
        }
    }
    fn run(&mut self, check_for_new_sessions: bool) -> Result<(), ScrError> {
        loop {
            if let Some(job) = self.find_job() {
                let res = self.run_job(job);
                self.cleanup_job_data();
                res?;
            } else {
                if !check_for_new_sessions || !self.get_next_session() {
                    return Ok(());
                }
            }
        }
    }
    fn get_next_session(&mut self) -> bool {
        let mut session_data = self.session.session_data.lock().unwrap();
        loop {
            if session_data.terminate {
                return false;
            }
            if session_data.generation != self.session_generation {
                self.session_generation = session_data.generation;
                self.stealers.extend(
                    session_data
                        .stealers
                        .iter()
                        .skip(self.stealers.len())
                        .map(|s| s.clone()),
                );
                self.ctx = session_data.ctx_data.clone();
                return true;
            }
            session_data = self.session.tasks_available.wait(session_data).unwrap();
        }
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
    fn setup_transform_stack(&mut self, job: &Job) -> Result<(), ScrError> {
        debug_assert!(self.tf_stack.is_empty());
        if job.is_stdin {
            self.tf_stack.push(Box::new(TfReadStdin::new()))
        } else {
            self.tf_stack.push(Box::new(TfStart::new(
                job.data.as_ref().map_or(MatchDataKind::None, |d| d.kind()),
            )))
        }
        for (i, op_ref) in job.ops.iter().rev().enumerate() {
            let cn = &self.ctx.chains[op_ref.chain_id as usize];
            for i in op_ref.op_offset as usize..cn.operations.len() {
                let op = &self.ctx.operations[cn.operations[i] as usize];
                let tf = op.apply(*op_ref, self.tf_stack.as_mut_slice())?;
                let requires_eval = tf.requires_eval;
                self.tf_stack.push(tf);
                if requires_eval {
                    break;
                }
            }
            if i + 1 < job.ops.len() {
                let mut tf_base = TfBase::from_parent(&self.tf_stack[0]);
                tf_base.begin_of_chain = true;
                tf_base.tfs_index = self.tf_stack.len() as TransformStackIndex;
                self.tf_stack.push(Box::new(TfParent {
                    tf_base,
                    op_ref: OperationRef {
                        //this little lie is fine since we will never error on this one anyways
                        chain_id: 0,
                        op_offset: 0,
                    },
                    offset: self.tf_stack.len() as TransformStackIndex,
                }));
            }
        }
        Ok(())
    }

    fn setup_job_data(&mut self, job: &Job) -> Result<(), ScrError> {
        self.setup_transform_stack(&job)?;
        self.tf_matches.extend(
            std::iter::repeat_with(Default::default)
                .take(1usize.saturating_sub(self.tf_matches.len())),
        );
        self.tf_outputs.extend(
            std::iter::repeat_with(Default::default)
                .take(self.tf_stack.len().saturating_sub(self.tf_outputs.len())),
        );
        Ok(())
    }
    fn cleanup_job_data(&mut self) {
        for mc in self.tf_matches.iter_mut().take(self.tf_stack.len()) {
            mc.args.clear();
            mc.ref_count = 0;
        }
        for tfo in self.tf_outputs.iter_mut().take(self.tf_stack.len()) {
            tfo.tfos.clear();
            tfo.next_dependant_indices.clear();
        }
        self.tf_stack.clear();
    }
    fn run_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.setup_job_data(&job)?;
        let mut last_tf_with_pending_output = 0;
        self.tf_matches[0].ref_count = 1;
        self.tf_matches[0].args = job.args;

        self.tf_outputs[0].tfos.push_back(TransformOutput {
            match_index: 0 as MatchIdx,
            data: job.data,
            args: Vec::default(),
            is_last_chunk: None,
        });
        self.tf_outputs[0].next_dependant_indices.push_back(0);

        loop {
            let tf_idx = last_tf_with_pending_output;
            let (tf_outputs_head, tf_outputs_tail) = self.tf_outputs.split_at_mut(tf_idx + 1);
            if let Some((tfo, next_dep_index)) = tf_outputs_head[tf_idx].front_mut() {
                let match_idx = tfo.match_index as usize;
                if *next_dep_index >= self.tf_stack[tf_idx].dependants.len() {
                    let rc = &mut self.tf_matches[match_idx].ref_count;
                    *rc -= 1;
                    if *rc == 0 {
                        self.tf_matches[match_idx].args.clear();
                    }
                    let tfo_ctx = &mut self.tf_outputs[last_tf_with_pending_output];
                    tfo_ctx.tfos.pop_front();
                    tfo_ctx.next_dependant_indices.pop_front();
                    continue;
                }
                let dep_idx = self.tf_stack[tf_idx].dependants[*next_dep_index] as usize;
                *next_dep_index += 1;
                let tfos_ctx = &mut tf_outputs_tail[dep_idx - tf_idx - 1];
                let tfos_len_prev = tfos_ctx.tfos.len();
                self.tf_stack[dep_idx].process(
                    &self.ctx,
                    &self.tf_matches[match_idx].args,
                    tfo,
                    &mut tfos_ctx.tfos,
                )?;
                debug_assert!(tfos_ctx.tfos.len() >= tfos_len_prev);
                let added_tfo_count = tfos_ctx.tfos.len() - tfos_len_prev;
                if added_tfo_count != 0 {
                    tfos_ctx
                        .next_dependant_indices
                        .extend(std::iter::repeat(0).take(added_tfo_count));
                    for res_tfo in tfos_ctx.tfos.iter().skip(tfos_len_prev) {
                        let mi = res_tfo.match_index as usize;
                        while mi >= self.tf_matches.len() {
                            self.tf_matches.push(Default::default());
                        }
                        self.tf_matches[mi].ref_count += 1;
                    }
                    if dep_idx > last_tf_with_pending_output {
                        last_tf_with_pending_output = dep_idx;
                    }
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
