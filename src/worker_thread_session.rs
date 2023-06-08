use std::{
    collections::{HashSet, VecDeque},
    iter,
};

use regex::Regex;

use crate::{
    context::SessionData,
    match_set::{FieldId, MatchSet, FIELD_ID_INPUT},
    operations::{format::TfFormat, operator_base::OperatorId, operator_data::OperatorData},
    scr_error::ScrError,
    worker_thread::{Job, JobData},
};

pub type TransformId = usize;

#[derive(Default)]
struct FieldInterest {
    // number of tfs that might read this field
    // if this drops to zero, remove the field
    ref_count: usize,
    permanent: HashSet<TransformId>,
    temporary: Vec<TransformId>,
}

pub struct WorkerThreadSession<'a> {
    session_data: &'a SessionData,

    match_set: MatchSet,
    transforms: Vec<TransformState<'a>>,
    unused_transform_ids: Vec<TransformId>,

    ready_queue: VecDeque<(TransformId, FieldId)>,
    field_interest: Vec<FieldInterest>,
}

struct TransformState<'a> {
    ready: bool,
    data: TransformData<'a>,
}

pub enum TransformData<'a> {
    Disabled,
    Print,
    Split(&'a [OperatorId]),
    Regex(Regex),
    Format(TfFormat<'a>),
}

impl<'a> WorkerThreadSession<'a> {
    pub fn new(sess: &'a SessionData) -> Self {
        WorkerThreadSession {
            session_data: sess,
            match_set: Default::default(),
            transforms: Default::default(),
            unused_transform_ids: Default::default(),
            ready_queue: Default::default(),
            field_interest: Default::default(),
        }
    }
    fn add_transform(&mut self, tf: TransformState<'a>) -> TransformId {
        if let Some(unused_id) = self.unused_transform_ids.pop() {
            self.transforms[unused_id] = tf;
            unused_id
        } else {
            let id = self.transforms.len() as TransformId;
            self.transforms.push(tf);
            id
        }
    }
    fn remove_transform(&mut self, tf_id: TransformId, triggering_field: FieldId) {
        self.transforms[tf_id as usize].data = TransformData::Disabled;
        self.unused_transform_ids.push(tf_id);
        let fi = &mut self.field_interest[triggering_field as usize];

        fi.permanent.remove(&triggering_field);
        fi.ref_count -= 1;
        if fi.ref_count == 0 {
            self.match_set.remove_field(triggering_field);
        }
    }
    #[allow(dead_code)] //TODO
    fn inform_field_filled(&mut self, field: FieldId) {
        let fi = &mut self.field_interest[field as usize];
        for tf in fi.permanent.iter().chain(fi.temporary.iter()) {
            self.ready_queue.push_back((*tf, field));
        }
        fi.temporary.clear();
    }
    fn setup_transforms_from_op(
        &mut self,
        start_ready: bool,
        start_op_id: OperatorId,
        input_field_id: FieldId,
    ) -> TransformId {
        let mut start_tf_id = None;
        let start_op = &self.session_data.operator_bases[start_op_id as usize];
        for op_id in &self.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..]
        {
            let op_data = &self.session_data.operator_data[*op_id as usize];
            let tf_data = match op_data {
                OperatorData::Print => TransformData::Print,
                OperatorData::Split(sd) => TransformData::Split(sd.target_operators.as_slice()),
                OperatorData::Regex(rd) => TransformData::Regex(rd.regex.clone()),
                OperatorData::Format(fmt) => TransformData::Format(TfFormat { parts: &fmt.parts }),
            };
            if start_tf_id.is_none() {
                let id = self.add_transform(TransformState {
                    ready: start_ready,
                    data: tf_data,
                });
                start_tf_id = Some(id);
                if start_ready {
                    self.ready_queue.push_back((id, input_field_id));
                }
            } else {
                self.add_transform(TransformState {
                    ready: false,
                    data: tf_data,
                });
            }
        }
        start_tf_id.unwrap()
    }

    fn setup_job(&mut self, job: Job) {
        match job.data {
            JobData::MatchSet(ms) => {
                self.match_set = ms;
            }
            JobData::Documents(_docs) => {
                self.match_set = Default::default();
                todo!()
            }
            JobData::DocumentIds(_ids) => {
                self.match_set = Default::default();
                todo!()
            }
            JobData::Stdin => {
                self.match_set = Default::default();
                todo!()
            }
        }
        self.ready_queue.clear();
        self.transforms.clear();
        self.field_interest.clear();
        self.field_interest
            .extend(iter::repeat_with(|| Default::default()).take(self.match_set.field_count()));

        for op in job.starting_ops {
            self.setup_transforms_from_op(true, op, FIELD_ID_INPUT);
        }
    }

    pub(crate) fn run_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.setup_job(job);

        while !self.match_set.is_empty() {
            self.ready_queue.push_back((0, FIELD_ID_INPUT));
            while let Some((tf_id, triggering_field_id)) = self.ready_queue.pop_front() {
                let tf = &mut self.transforms[tf_id as usize];
                tf.ready = false;
                match tf.data {
                    TransformData::Disabled => unreachable!(),
                    TransformData::Split(targets) => {
                        for op_id in targets {
                            self.setup_transforms_from_op(true, *op_id, triggering_field_id);
                        }
                        self.remove_transform(tf_id, triggering_field_id);
                    }
                    TransformData::Print => todo!(),
                    TransformData::Regex(_) => todo!(),
                    TransformData::Format(_) => todo!(),
                }
            }
        }

        Ok(())
    }
}
