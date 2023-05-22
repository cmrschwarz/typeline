use std::collections::{HashMap, VecDeque};
use std::iter;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use crossbeam::deque::{Stealer, Worker};
use smallvec::SmallVec;

use crate::context::{ContextData, SessionData};
use crate::match_data::{MatchData, MatchDataKind};
use crate::operations::operation::OperationRef;
use crate::operations::parent::TfParent;
use crate::operations::read_stdin::TfReadStdin;
use crate::operations::start::TfStart;
use crate::operations::transform::{
    MatchIdx, TfBase, Transform, TransformOutput, TransformStackIndex,
};
use crate::scr_error::ScrError;

pub(crate) struct Job {
    pub(crate) ops: SmallVec<[OperationRef; 2]>,
    pub(crate) data: Option<MatchData>,
    pub(crate) args: HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
    pub(crate) is_stdin: bool,
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

pub(crate) struct WorkerThread {
    pub(crate) context_data: Arc<ContextData>,
    pub(crate) worker: Worker<Job>,
    pub(crate) stealers: Vec<Stealer<Job>>,

    // aquired from context_data->session at the start of each generation
    session_generation: usize,
    session_data: Arc<SessionData>,

    tf_matches: Vec<MatchContext>,
    tf_outputs: Vec<TransformOutputsContext>,
}

struct WorkerThreadSession<'a> {
    session_data: &'a SessionData,
    tf_stack: Vec<Box<dyn Transform + 'a>>,
}

impl WorkerThread {
    pub(crate) fn new(index: usize, worker: Worker<Job>, context_data: Arc<ContextData>) -> Self {
        let sess = context_data.session.lock().unwrap();
        Self {
            worker: worker,
            session_data: sess.data.clone(),
            stealers: sess
                .stealers
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != index)
                .map(|(_, s)| s.clone())
                .collect(),
            session_generation: sess.generation,
            context_data: context_data.clone(),
            tf_outputs: Default::default(),
            tf_matches: Default::default(),
        }
    }
    pub(crate) fn run(&mut self, check_for_new_generations: bool) -> Result<(), ScrError> {
        let mut sess_data_arc = self.session_data.clone();
        let mut sess = ManuallyDrop::new(WorkerThreadSession {
            session_data: &sess_data_arc,
            tf_stack: Default::default(),
        });
        loop {
            if let Some(job) = self.find_job() {
                let res = self.run_job(job, &mut sess);
                self.cleanup_job_data(&mut sess);
                res?;
            } else {
                if !check_for_new_generations || !self.aquire_next_generation() {
                    return Ok(());
                }
                let _ = ManuallyDrop::into_inner(sess);
                sess_data_arc = self.session_data.clone();
                sess = ManuallyDrop::new(WorkerThreadSession {
                    session_data: &sess_data_arc,
                    tf_stack: Default::default(),
                });
            }
        }
    }
    fn aquire_next_generation(&mut self) -> bool {
        let mut sess = self.context_data.session.lock().unwrap();
        loop {
            if sess.terminate {
                return false;
            }
            if sess.generation != self.session_generation {
                self.session_generation = sess.generation;
                self.stealers.extend(
                    sess.stealers
                        .iter()
                        .skip(self.stealers.len())
                        .map(|s| s.clone()),
                );
                self.session_data = sess.data.clone();
                return true;
            }
            sess = self.context_data.tasks_available.wait(sess).unwrap();
        }
    }
    fn find_job(&mut self) -> Option<Job> {
        self.worker.pop().or_else(|| {
            iter::repeat_with(|| {
                self.context_data
                    .injector
                    .steal_batch_and_pop(&self.worker)
                    .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
            })
            .find(|s| !s.is_retry())
            .and_then(|s| s.success())
        })
    }
    fn setup_transform_stack<'a>(
        &mut self,
        job: &Job,
        sess: &mut WorkerThreadSession<'a>,
    ) -> Result<(), ScrError> {
        debug_assert!(sess.tf_stack.is_empty());
        if job.is_stdin {
            sess.tf_stack.push(Box::new(TfReadStdin::new()))
        } else {
            sess.tf_stack.push(Box::new(TfStart::new(
                job.data.as_ref().map_or(MatchDataKind::None, |d| d.kind()),
            )))
        }
        for (i, op_ref) in job.ops.iter().rev().enumerate() {
            let cn = &self.session_data.chains[op_ref.chain_id as usize];
            for i in op_ref.op_offset as usize..cn.operations.len() {
                let op = &sess.session_data.operations[cn.operations[i] as usize];
                let sl = sess.tf_stack.as_mut_slice();
                let tf = op.apply(*op_ref, sl)?;
                let requires_eval = tf.base().requires_eval;
                sess.tf_stack.push(tf);
                if requires_eval {
                    break;
                }
            }
            if i + 1 < job.ops.len() {
                let mut tf_base = TfBase::from_parent(&sess.tf_stack[0].base());
                tf_base.begin_of_chain = true;
                tf_base.tfs_index = sess.tf_stack.len() as TransformStackIndex;
                sess.tf_stack.push(Box::new(TfParent {
                    tf_base,
                    op_ref: OperationRef {
                        //this little lie is fine since we will never error on this one anyways
                        chain_id: 0,
                        op_offset: 0,
                    },
                    offset: sess.tf_stack.len() as TransformStackIndex,
                }));
            }
        }
        Ok(())
    }

    fn setup_job_data<'a>(
        &mut self,
        job: &Job,
        sess: &mut WorkerThreadSession<'a>,
    ) -> Result<(), ScrError> {
        self.setup_transform_stack(job, sess)?;
        self.tf_matches.extend(
            std::iter::repeat_with(Default::default)
                .take(1usize.saturating_sub(self.tf_matches.len())),
        );
        self.tf_outputs.extend(
            std::iter::repeat_with(Default::default)
                .take(sess.tf_stack.len().saturating_sub(self.tf_outputs.len())),
        );
        Ok(())
    }
    fn cleanup_job_data<'a>(&mut self, sess: &mut WorkerThreadSession<'a>) {
        for mc in self.tf_matches.iter_mut().take(sess.tf_stack.len()) {
            mc.args.clear();
            mc.ref_count = 0;
        }
        for tfo in self.tf_outputs.iter_mut().take(sess.tf_stack.len()) {
            tfo.tfos.clear();
            tfo.next_dependant_indices.clear();
        }
        sess.tf_stack.clear();
    }
    fn run_job<'a>(
        &mut self,
        job: Job,
        sess: &mut WorkerThreadSession<'a>,
    ) -> Result<(), ScrError> {
        self.setup_job_data(&job, sess)?;
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
            let tf_dep_count = sess.tf_stack[tf_idx].base().dependants.len();
            let (tf_outputs_head, tf_outputs_tail) = self.tf_outputs.split_at_mut(tf_idx + 1);
            if let Some((tfo, next_dep_index)) = tf_outputs_head[tf_idx].front_mut() {
                let match_idx = tfo.match_index as usize;
                if *next_dep_index >= tf_dep_count {
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
                let dep_idx = sess.tf_stack[tf_idx].base().dependants[*next_dep_index] as usize;
                *next_dep_index += 1;
                let tfos_ctx = &mut tf_outputs_tail[dep_idx - tf_idx - 1];
                let tfos_len_prev = tfos_ctx.tfos.len();
                sess.tf_stack[dep_idx].process(
                    &self.session_data,
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
