use std::{
    collections::{hash_map, BinaryHeap, HashMap, HashSet},
    iter,
    num::NonZeroUsize,
};

use regex::Regex;

use crate::{
    context::SessionData,
    field_data::{EntryId, FieldData},
    operations::{format::FormatPart, operator_base::OperatorId, operator_data::OperatorData},
    scr_error::ScrError,
    string_store::StringStoreEntry,
    universe::Universe,
    worker_thread::{Job, JobData},
};

pub type TransformId = usize;

#[derive(Default)]
struct Field {
    // number of tfs that might read this field
    // if this drops to zero, remove the field
    ref_count: usize,
    match_set: MatchSetId,
    permanent_interest: HashSet<TransformId>,
    temporary_interest: Vec<TransformId>,
    name: Option<StringStoreEntry>,
    // entry in the working set if this field is enabled
    // the indices field (id 0) is always enabled, but stores None here
    // so we can use NonZeroUsize
    #[allow(dead_code)] //TODO
    working_set_idx: Option<NonZeroUsize>,
    field_data: FieldData,
}

pub type FieldId = usize;

pub const FIELD_ID_JOB_INPUT_INDICES: FieldId = 0;
pub const FIELD_ID_JOB_INPUT: FieldId = 1;

type MatchSetId = usize;

#[derive(Default)]
#[repr(C)]
pub struct MatchSet {
    working_set_updates: Vec<(EntryId, FieldId)>,
    working_set: Vec<FieldId>,
    field_name_map: HashMap<StringStoreEntry, HashSet<FieldId>>,
}

pub struct WorkerThreadSession<'a> {
    session_data: &'a SessionData,

    match_sets: Universe<MatchSetId, MatchSet>,
    transforms: Universe<TransformId, TransformState<'a>>,
    fields: Universe<FieldId, Field>,

    ready_queue: BinaryHeap<TransformId>,
}

struct TransformState<'a> {
    ready: bool,
    data: TransformData<'a>,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub parts: &'a [FormatPart],
}

pub enum TransformData<'a> {
    Disabled,
    Print,
    Split(TransformId, &'a [OperatorId]),
    Regex(Regex),
    Format(TfFormat<'a>),
}

impl<'a> WorkerThreadSession<'a> {
    pub fn new(sess: &'a SessionData) -> Self {
        WorkerThreadSession {
            session_data: sess,
            match_sets: Default::default(),
            transforms: Default::default(),
            ready_queue: Default::default(),
            fields: Default::default(),
        }
    }
    pub fn add_field(&mut self, ms_id: MatchSetId, name: Option<StringStoreEntry>) -> FieldId {
        let field_id = self.fields.claim();
        let field = &mut self.fields[field_id as FieldId];
        field.name = name;
        field.match_set = ms_id;
        if let Some(name) = name {
            match self.match_sets[ms_id].field_name_map.entry(name) {
                hash_map::Entry::Occupied(ref mut e) => {
                    e.get_mut().insert(field_id);
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(HashSet::from_iter(iter::once(field_id)));
                }
            }
        }
        field_id
    }
    pub fn remove_field(&mut self, id: FieldId) {
        let field = &mut self.fields[id];
        let match_set = &mut self.match_sets[field.match_set];
        if let Some(ref name) = field.name {
            match_set.field_name_map.get_mut(name).unwrap().remove(&id);
        }
        field.permanent_interest.clear();
        field.temporary_interest.clear();
        field.ref_count = 0;
        field.field_data.clear();
        self.fields.release(id);
    }
    fn add_transform(&mut self, tf: TransformState<'a>) -> TransformId {
        self.transforms.push(tf)
    }
    fn remove_transform(&mut self, tf_id: TransformId, triggering_field: FieldId) {
        self.transforms[tf_id].data = TransformData::Disabled;
        self.transforms.release(tf_id);
        let field = &mut self.fields[triggering_field];

        field.permanent_interest.remove(&tf_id);
        field.ref_count -= 1;
        if field.ref_count == 0 {
            self.remove_field(triggering_field);
        }
    }
    fn add_match_set(&mut self) -> MatchSetId {
        let id = self.match_sets.claim();
        id
    }
    #[allow(dead_code)] //TODO
    fn inform_field_filled(&mut self, field_id: FieldId) {
        let field = &mut self.fields[field_id];
        for tf in field
            .permanent_interest
            .iter()
            .chain(field.temporary_interest.iter())
        {
            self.ready_queue.push(*tf);
        }
        field.temporary_interest.clear();
    }
    fn setup_transforms_from_op(
        &mut self,
        start_ready: bool,
        start_op_id: OperatorId,
        input_field_id: FieldId,
    ) -> TransformId {
        let ms_id = self.add_match_set();
        let mut start_tf_id = None;
        let start_op = &self.session_data.operator_bases[start_op_id as usize];
        let mut prev_field_id = input_field_id;
        for op_id in &self.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..]
        {
            let op_data = &self.session_data.operator_data[*op_id as usize];
            let tf_data = match op_data {
                OperatorData::Print => TransformData::Print,
                OperatorData::Split(sd) => {
                    TransformData::Split(prev_field_id, sd.target_operators.as_slice())
                }
                OperatorData::Regex(rd) => TransformData::Regex(rd.regex.clone()),
                OperatorData::Format(fmt) => {
                    let output_field = self.add_field(ms_id, None);
                    prev_field_id = output_field;
                    TransformData::Format(TfFormat {
                        output_field,
                        parts: &fmt.parts,
                    })
                }
            };
            if start_tf_id.is_none() {
                let id = self.add_transform(TransformState {
                    ready: start_ready,
                    data: tf_data,
                });
                start_tf_id = Some(id);
                if start_ready {
                    self.ready_queue.push(id);
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
        self.match_sets.clear();
        self.match_sets.push(Default::default());
        let ms = &mut self.match_sets[0];
        ms.working_set
            .extend_from_slice([FIELD_ID_JOB_INPUT_INDICES, FIELD_ID_JOB_INPUT].as_slice());
        self.fields.clear();
        match job.data {
            JobData::FieldData(_fd) => loop {
                break; //TODO
            },
            JobData::Documents(_docs) => {
                todo!()
            }
            JobData::DocumentIds(_ids) => {
                todo!()
            }
            JobData::Stdin => {
                todo!()
            }
        }
        self.ready_queue.clear();

        for op in job.starting_ops {
            self.setup_transforms_from_op(true, op, FIELD_ID_JOB_INPUT);
        }
    }

    pub(crate) fn run_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.setup_job(job);

        while !self.fields[FIELD_ID_JOB_INPUT].field_data.is_empty() {
            self.ready_queue.push(0 as TransformId);
            while let Some(tf_id) = self.ready_queue.pop() {
                let tf = &mut self.transforms[tf_id];
                tf.ready = false;
                match tf.data {
                    TransformData::Disabled => unreachable!(),
                    TransformData::Split(input_field_id, targets) => {
                        for op_id in targets {
                            self.setup_transforms_from_op(true, *op_id, input_field_id);
                        }
                        self.remove_transform(tf_id, input_field_id);
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
