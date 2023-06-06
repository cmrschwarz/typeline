use std::collections::VecDeque;
use std::iter;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use crossbeam::deque::{Stealer, Worker};
use smallvec::SmallVec;

use crate::context::{ContextData, SessionData};
use crate::match_set::{MatchSet, MatchSetShared};
use crate::operations::operator_base::OperatorId;
use crate::operations::operator_data::TransformData;
use crate::scr_error::ScrError;

pub(crate) struct Job {
    pub starting_ops: SmallVec<[OperatorId; 2]>,
    pub match_sets: SmallVec<[MatchSetShared; 1]>,
}

pub(crate) struct WorkerThread {
    pub(crate) context_data: Arc<ContextData>,
    pub(crate) worker: Worker<Job>,
    pub(crate) stealers: Vec<Stealer<Job>>,

    // aquired from context_data->session at the start of each generation
    session_generation: usize,
    session_data: Arc<SessionData>,
}

struct WorkerThreadSession<'a> {
    session_data: &'a SessionData,
    tf_data: Vec<TransformData<'a>>,
    match_sets: Vec<VecDeque<MatchSet>>,
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
        }
    }
    pub(crate) fn run(&mut self, check_for_new_generations: bool) -> Result<(), ScrError> {
        let mut sess_data_arc = self.session_data.clone();
        let mut sess = ManuallyDrop::new(WorkerThreadSession {
            session_data: &sess_data_arc,
            tf_data: Default::default(),
            match_sets: Default::default(),
        });
        loop {
            if let Some(job) = self.find_job() {
                let res = sess.run_job(job);
                res?;
            } else {
                if !check_for_new_generations || !self.aquire_next_generation() {
                    return Ok(());
                }
                let _ = ManuallyDrop::into_inner(sess);
                sess_data_arc = self.session_data.clone();
                sess = ManuallyDrop::new(WorkerThreadSession {
                    session_data: &sess_data_arc,
                    tf_data: Default::default(),
                    match_sets: Default::default(),
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
}

impl<'a> WorkerThreadSession<'a> {
    fn run_job(&mut self, mut job: Job) -> Result<(), ScrError> {
        self.match_sets[0].extend(job.match_sets.into_iter().map(MatchSet::from));

        Ok(())
    }
}
