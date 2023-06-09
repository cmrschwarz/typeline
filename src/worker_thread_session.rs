use std::{
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
    iter,
};

use nonmax::NonMaxUsize;
use regex::Regex;
use smallvec::{smallvec, SmallVec};

use crate::{
    context::SessionData,
    field_data::{EntryId, FieldData},
    operations::{format::FormatPart, operator_base::OperatorId, operator_data::OperatorData},
    scr_error::ScrError,
    scratch_vec::ScratchVec,
    string_store::StringStoreEntry,
    universe::Universe,
    worker_thread::{Job, JobData},
};

pub type TransformId = NonMaxUsize;

#[derive(Default)]
struct Field {
    // number of tfs that might read this field
    // if this drops to zero, remove the field
    // TODO:
    // (unless this is named, we have a fwd, and this is not shadowed before it)
    ref_count: usize,
    match_set: MatchSetId,
    shadowed_after_tf: TransformId,

    name: Option<StringStoreEntry>,
    #[allow(dead_code)] //TODO
    working_set_idx: Option<NonMaxUsize>,
    field_data: FieldData,
}

pub type FieldId = NonMaxUsize;

pub const FIELD_ID_JOB_INPUT_INDICES: FieldId = unsafe { NonMaxUsize::new_unchecked(0) };
pub const FIELD_ID_JOB_INPUT: FieldId = unsafe { NonMaxUsize::new_unchecked(1) };

type MatchSetId = NonMaxUsize;

#[derive(Default)]
#[repr(C)]
pub struct MatchSet {
    stream_batch_size: usize,
    stream_participants: Vec<TransformId>,
    working_set_updates: Vec<(EntryId, FieldId)>,
    //should not contain tf input fields (?)
    working_set: Vec<FieldId>,
    field_name_map: HashMap<StringStoreEntry, VecDeque<FieldId>>,
}

pub struct WorkerThreadSession<'a> {
    session_data: &'a SessionData,

    match_sets: Universe<MatchSetId, MatchSet>,
    transforms: Universe<TransformId, TransformState<'a>>,
    fields: Universe<FieldId, Field>,
    stream_producers: Vec<TransformId>,

    scrach_memory: Vec<&'static u8>,

    ready_queue: BinaryHeap<TransformId>,
}

struct TransformState<'a> {
    successor: Option<TransformId>,
    stream_successor: Option<TransformId>,
    stream_producers_slot_index: usize,
    input_field: FieldId,
    available_batch_size: usize,
    match_set_id: MatchSetId,
    data: TransformData<'a>,
}

pub struct TfFormat<'a> {
    pub output_field: FieldId,
    pub parts: &'a [FormatPart],
}

pub struct TfSplit {
    expanded: bool,
    forwarded_stream_elements: usize,
    // Operator Ids before expansion, transform ids after
    targets: Vec<NonMaxUsize>,
    field_names_set: HashMap<StringStoreEntry, SmallVec<[FieldId; 2]>>,
}

pub enum TransformData<'a> {
    Disabled,
    Print,
    Split(Option<TfSplit>),
    Regex(Regex),
    Format(TfFormat<'a>),
}

impl Default for TransformData<'_> {
    fn default() -> Self {
        Self::Disabled
    }
}

impl<'a> WorkerThreadSession<'a> {
    pub fn new(sess: &'a SessionData) -> Self {
        WorkerThreadSession {
            session_data: sess,
            match_sets: Default::default(),
            transforms: Default::default(),
            ready_queue: Default::default(),
            fields: Default::default(),
            stream_producers: Default::default(),
            scrach_memory: Default::default(),
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
                    e.get_mut().push_back(field_id);
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(VecDeque::from_iter(iter::once(field_id)));
                }
            }
        }
        field_id
    }
    pub fn remove_field(&mut self, id: FieldId) {
        let field = &mut self.fields[id];
        let match_set = &mut self.match_sets[field.match_set];
        if let Some(ref name) = field.name {
            let refs = match_set.field_name_map.get_mut(name).unwrap();
            debug_assert!(*refs.front().unwrap() == id);
            refs.pop_front();
        }
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

        field.ref_count -= 1;
        if field.ref_count == 0 {
            self.remove_field(triggering_field);
        }
    }
    fn add_match_set(&mut self) -> MatchSetId {
        let id = self.match_sets.claim();
        id
    }
    fn setup_transforms_from_op(
        &mut self,
        match_set_id: MatchSetId,
        available_batch_size: usize,
        start_op_id: OperatorId,
        input_field_id: FieldId,
    ) -> TransformId {
        let mut start_tf_id = None;
        let start_op = &self.session_data.operator_bases[start_op_id as usize];
        let mut prev_tf = None;
        let mut prev_field_id = input_field_id;
        let mut output_field = input_field_id;
        for op_id in &self.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..]
        {
            let op_data = &self.session_data.operator_data[*op_id as usize];
            let data = match op_data {
                OperatorData::Print => TransformData::Print,
                OperatorData::Split(sd) => TransformData::Split(Some(TfSplit {
                    expanded: false,
                    targets: sd
                        .target_operators
                        .iter()
                        .map(|op| (*op as usize).try_into().unwrap())
                        .collect(),
                    forwarded_stream_elements: 0,
                    field_names_set: Default::default(),
                })),
                OperatorData::Regex(rd) => TransformData::Regex(rd.regex.clone()),
                OperatorData::Format(fmt) => {
                    output_field = self.add_field(match_set_id, None);
                    TransformData::Format(TfFormat {
                        output_field,
                        parts: &fmt.parts,
                    })
                }
            };

            let tf_id = if start_tf_id.is_none() {
                let id = self.add_transform(TransformState {
                    available_batch_size,
                    data,
                    input_field: prev_field_id,
                    match_set_id,
                    stream_successor: None,
                    stream_producers_slot_index: 0,
                    successor: None,
                });
                start_tf_id = Some(id);
                if available_batch_size > 0 {
                    self.ready_queue.push(id);
                }
                id
            } else {
                self.add_transform(TransformState {
                    available_batch_size,
                    data,
                    input_field: prev_field_id,
                    match_set_id,
                    stream_successor: None,
                    stream_producers_slot_index: 0,
                    successor: None,
                })
            };
            if let Some(tf_id) = prev_tf {
                self.transforms[tf_id].successor = Some(tf_id);
            }
            prev_tf = Some(tf_id);
            prev_field_id = output_field;
        }
        start_tf_id.unwrap()
    }

    fn setup_job(&mut self, job: Job) {
        self.match_sets.clear();
        self.match_sets.push(Default::default());
        let ms = &mut self.match_sets[0usize.try_into().unwrap()];
        ms.working_set
            .extend_from_slice([FIELD_ID_JOB_INPUT_INDICES, FIELD_ID_JOB_INPUT].as_slice());
        self.fields.clear();
        let entry_count: usize = 0;
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

        if job.starting_ops.len() > 1 {
            let ms_id = self.add_match_set();
            self.add_transform(TransformState {
                input_field: FIELD_ID_JOB_INPUT,
                data: TransformData::Split(Some(TfSplit {
                    expanded: false,
                    forwarded_stream_elements: 0,
                    targets: job
                        .starting_ops
                        .iter()
                        .map(|op| (*op as usize).try_into().unwrap())
                        .collect(),
                    field_names_set: Default::default(),
                })),
                available_batch_size: entry_count,
                stream_producers_slot_index: 0,
                stream_successor: None,
                match_set_id: ms_id,
                successor: None,
            });
        } else {
            self.setup_transforms_from_op(0usize.try_into().unwrap(), 0, 0, FIELD_ID_JOB_INPUT);
        }
    }
    fn handle_split(&mut self, tf_id: TransformId, mut s: TfSplit) -> TfSplit {
        let tf = &mut self.transforms[tf_id];
        let tf_ms_id = tf.match_set_id;
        let bs = tf.available_batch_size;
        tf.available_batch_size = 0;
        if !s.expanded {
            for i in 0..s.targets.len() {
                let op = s.targets[i];
                let ms_id = self.add_match_set();
                let input_field = self.add_field(tf_ms_id, None);
                let _tf = self.setup_transforms_from_op(
                    ms_id,
                    0,
                    <NonMaxUsize as TryInto<usize>>::try_into(op).unwrap() as OperatorId,
                    input_field,
                );
            }
        }
        if self.match_sets[tf_ms_id].stream_batch_size != 0 {
            todo!();
        } else {
            //TODO: detect invalidations somehow instead
            s.field_names_set.clear();
            //TODO: do something clever, per target, cow, etc. instead of this dumb copy
            for field_id in self.match_sets[tf_ms_id].working_set.iter() {
                // we should only have named fields in the working set (?)
                if let Some(name) = self.fields[*field_id].name {
                    s.field_names_set
                        .entry(name)
                        .or_insert_with(|| smallvec![])
                        .push(*field_id);
                }
            }
            for (name, targets) in &mut s.field_names_set {
                let source_id = *self.match_sets[tf_ms_id]
                    .field_name_map
                    .get(&name)
                    .unwrap()
                    .back()
                    .unwrap();
                let mut offset: usize = 0;
                let mut source = None;
                let mut rem = self.fields.as_mut_slice();

                // the following is a annoyingly complicated way to gathering a
                // list of mutable references from a list of indices without using unsafe
                let mut targets_arr = ScratchVec::new(&mut self.scrach_memory);
                targets.sort();

                for i in targets.iter() {
                    let (tgt, rem_new) = rem.split_at_mut(usize::from(*i) as usize - offset + 1);
                    offset += tgt.len();
                    let fd = &mut tgt[tgt.len() - 1].field_data;
                    if source_id == *i {
                        source = Some(fd);
                    } else {
                        targets_arr.push(fd);
                    }
                    rem = rem_new;
                }

                source.unwrap().copy_n(bs, targets_arr.as_slice());
            }
        }
        s
    }
    pub(crate) fn run_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.setup_job(job);
        while !self.fields[FIELD_ID_JOB_INPUT].field_data.is_empty() {
            self.ready_queue.push(0usize.try_into().unwrap()); //TODO
            while let Some(tf_id) = self.ready_queue.peek().map(|t| *t) {
                let tf = &mut self.transforms[tf_id];

                match tf.data {
                    TransformData::Disabled => unreachable!(),
                    TransformData::Split(ref mut split_orig) => {
                        let mut s = split_orig.take().unwrap();
                        s = self.handle_split(tf_id, s);
                        if let TransformData::Split(ref mut s_restored) =
                            self.transforms[tf_id].data
                        {
                            *s_restored = Some(s);
                        }
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
