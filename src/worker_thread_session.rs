use std::{
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
    iter,
};

use nonmax::NonMaxUsize;

use crate::{
    context::SessionData,
    document::DocumentSource,
    field_data::{EntryId, FieldData},
    operations::{
        format::setup_tf_format,
        operator_base::OperatorId,
        operator_data::OperatorData,
        print::handle_print_batch_mode,
        split::{handle_split, setup_tf_split, setup_ts_split_as_entry_point},
        transform_state::{TransformData, TransformState},
        OperatorSetupError,
    },
    scr_error::ScrError,
    string_store::StringStoreEntry,
    universe::Universe,
    worker_thread::{Job, JobData},
};

pub type TransformId = NonMaxUsize;

#[derive(Default)]
pub struct Field {
    // number of tfs that might read this field
    // if this drops to zero, remove the field
    // TODO:
    // (unless this is named, we have a fwd, and this is not shadowed before it)
    pub ref_count: usize,
    pub match_set: MatchSetId,
    pub shadowed_after_tf: TransformId,

    pub name: Option<StringStoreEntry>,
    #[allow(dead_code)] //TODO
    pub working_set_idx: Option<NonMaxUsize>,
    pub field_data: FieldData,
}

pub type FieldId = NonMaxUsize;

pub type MatchSetId = NonMaxUsize;

#[derive(Default)]
#[repr(C)]
pub struct MatchSet {
    pub stream_batch_size: usize,
    pub stream_participants: Vec<TransformId>,
    pub working_set_updates: Vec<(EntryId, FieldId)>,
    //should not contain tf input fields (?)
    pub working_set: Vec<FieldId>,
    pub field_name_map: HashMap<StringStoreEntry, VecDeque<FieldId>>,
}

pub struct WorkerThreadSession<'a> {
    pub session_data: &'a SessionData,
    pub transforms: Universe<TransformId, TransformState<'a>>,

    pub match_sets: Universe<MatchSetId, MatchSet>,
    pub fields: Universe<FieldId, Field>,
    pub stream_producers: Vec<TransformId>,

    pub ready_queue: BinaryHeap<TransformId>,

    pub scrach_memory: Vec<&'static u8>,
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
    pub fn add_transform(&mut self, tf: TransformState<'a>) -> TransformId {
        self.transforms.push(tf)
    }
    pub fn remove_transform(&mut self, tf_id: TransformId, triggering_field: FieldId) {
        self.transforms[tf_id].data = TransformData::Disabled;
        self.transforms.release(tf_id);
        let field = &mut self.fields[triggering_field];

        field.ref_count -= 1;
        if field.ref_count == 0 {
            self.remove_field(triggering_field);
        }
    }
    pub fn add_match_set(&mut self) -> MatchSetId {
        let id = self.match_sets.claim();
        id
    }
    pub fn setup_transforms_from_op(
        &mut self,
        match_set_id: MatchSetId,
        available_batch_size: usize,
        start_op_id: OperatorId,
        input_field_id: FieldId,
    ) -> TransformId {
        let mut start_tf_id = None;
        let start_op = &self.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.session_data.chains[start_op.chain_id as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = None;
        let mut prev_field_id = input_field_id;
        let mut output_field = input_field_id;
        for op_id in &self.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..]
        {
            let op_data = &self.session_data.operator_data[*op_id as usize];
            let data = match op_data {
                OperatorData::Print => TransformData::Print,
                OperatorData::Split(ref sd) => TransformData::Split(setup_tf_split(sd)),
                OperatorData::Regex(rd) => TransformData::Regex(rd.regex.clone()),
                OperatorData::Format(ref fmt) => {
                    output_field = self.add_field(match_set_id, None);
                    TransformData::Format(setup_tf_format(self, output_field, fmt))
                }
            };
            let ts_state = TransformState {
                available_batch_size,
                data,
                input_field: prev_field_id,
                match_set_id,
                stream_successor: None,
                stream_producers_slot_index: None,
                desired_batch_size: default_batch_size,
                successor: None,
            };
            let tf_id = if start_tf_id.is_none() {
                let id = self.add_transform(ts_state);
                start_tf_id = Some(id);
                if available_batch_size > 0 {
                    self.ready_queue.push(id);
                }
                id
            } else {
                self.add_transform(ts_state)
            };
            if let Some(tf_id) = prev_tf {
                self.transforms[tf_id].successor = Some(tf_id);
            }
            prev_tf = Some(tf_id);
            prev_field_id = output_field;
        }
        start_tf_id.unwrap()
    }

    fn setup_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.match_sets.clear();
        self.fields.clear();
        self.ready_queue.clear();
        let ms_id = self.add_match_set();
        let input_data = self.add_field(ms_id, None);
        let mut entry_count: usize = 0;
        match job.data {
            JobData::FieldData(_fd) => loop {
                break; //TODO
            },
            JobData::Documents(_docs) => {
                todo!()
            }
            JobData::DocumentIds(doc_ids) => {
                for ds in doc_ids
                    .iter()
                    .rev()
                    .map(|d| &self.session_data.documents[*d as usize].source)
                {
                    match ds {
                        DocumentSource::Url(_) => todo!(),
                        DocumentSource::File(_) => todo!(),
                        DocumentSource::Bytes(_) => todo!(),
                        DocumentSource::Stdin => todo!(),
                        DocumentSource::String(str) => {
                            self.fields[input_data].field_data.push_str(str, 1);
                            entry_count += 1;
                        }
                    }
                }
            }
            JobData::Stdin => {
                todo!()
            }
        }
        if entry_count == 0 {
            return Err(OperatorSetupError::new(
                "must supply at least one initial data element",
                job.starting_ops[0],
            )
            .into());
        }
        assert!(!job.starting_ops.is_empty());
        if job.starting_ops.len() > 1 {
            let tf = setup_ts_split_as_entry_point(
                self,
                input_data,
                ms_id,
                entry_count,
                job.starting_ops.iter(),
            );
            let tf_id = self.add_transform(tf);
            self.ready_queue.push(tf_id);
        } else {
            self.setup_transforms_from_op(ms_id, entry_count, job.starting_ops[0], input_data);
        }
        Ok(())
    }

    fn run_batch_mode(&mut self) -> Result<bool, ScrError> {
        while let Some(tf_id) = self.ready_queue.peek().map(|t| *t) {
            let tf = &mut self.transforms[tf_id];

            match tf.data {
                TransformData::Disabled => unreachable!(),
                TransformData::Split(ref mut split_orig) => {
                    let mut s = split_orig.take().unwrap();
                    s = handle_split(self, false, tf_id, s);
                    match self.transforms[tf_id].data {
                        TransformData::Split(ref mut s_restored) => *s_restored = Some(s),
                        _ => unreachable!(),
                    }
                }
                TransformData::Print => {
                    handle_print_batch_mode(self, tf_id);
                }
                TransformData::Regex(_) => todo!(),
                TransformData::Format(_) => todo!(),
            }
        }

        Ok(true)
    }
    fn run_stream_mode(&mut self) -> Result<bool, ScrError> {
        while let Some(tf_id) = self.ready_queue.peek().map(|t| *t) {
            let tf = &mut self.transforms[tf_id];

            match tf.data {
                TransformData::Disabled => unreachable!(),
                TransformData::Split(ref mut split_orig) => {
                    let mut s = split_orig.take().unwrap();
                    s = handle_split(self, true, tf_id, s);
                    match self.transforms[tf_id].data {
                        TransformData::Split(ref mut s_restored) => *s_restored = Some(s),
                        _ => unreachable!(),
                    }
                }
                TransformData::Print => {
                    todo!();
                }
                TransformData::Regex(_) => todo!(),
                TransformData::Format(_) => todo!(),
            }
        }
        Ok(true)
    }
    pub(crate) fn run_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.setup_job(job)?;
        loop {
            if self.run_batch_mode()? == true {
                break;
            }
            if self.run_stream_mode()? == true {
                break;
            }
        }

        Ok(())
    }
}
