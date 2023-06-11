use std::{
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
    iter,
    ops::Deref,
    ops::DerefMut,
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
        regex::{handle_tf_regex_batch_mode, setup_tf_regex},
        split::{handle_tf_split, setup_tf_split, setup_ts_split_as_entry_point},
        transform_state::{TransformData, TransformState},
        OperatorSetupError,
    },
    scr_error::ScrError,
    string_store::StringStoreEntry,
    universe::Universe,
    worker_thread::{Job, JobInput},
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
    pub transform_data: Vec<TransformData<'a>>,
    pub job_data: JobData<'a>,
}

impl<'a> Deref for WorkerThreadSession<'a> {
    type Target = JobData<'a>;

    fn deref(&self) -> &Self::Target {
        &self.job_data
    }
}
impl<'a> DerefMut for WorkerThreadSession<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.job_data
    }
}

pub struct JobData<'a> {
    pub transforms: Universe<TransformId, TransformState>,
    pub session_data: &'a SessionData,
    pub match_sets: Universe<MatchSetId, MatchSet>,
    pub fields: Universe<FieldId, Field>,
    pub stream_producers: Vec<TransformId>,

    pub ready_queue: BinaryHeap<TransformId>,

    pub scratch_memory: Vec<&'static u8>,
}

impl<'a> JobData<'a> {
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
    pub fn add_match_set(&mut self) -> MatchSetId {
        let id = self.match_sets.claim();
        id
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    pub fn inform_successor_batch_available(&mut self, tf_id: TransformId, batch_size: usize) {
        if let Some(succ_tf_id) = self.transforms[tf_id].successor {
            let succ = &mut self.transforms[succ_tf_id];
            if succ.available_batch_size == 0 {
                self.ready_queue.push(succ_tf_id);
            }
            succ.available_batch_size += batch_size;
        }
    }
}

impl<'a> WorkerThreadSession<'a> {
    fn setup_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.match_sets.clear();
        self.fields.clear();
        self.ready_queue.clear();
        let ms_id = self.add_match_set();
        let input_data = self.add_field(ms_id, None);
        let mut entry_count: usize = 0;

        match job.data {
            JobInput::FieldData(_fd) => loop {
                break; //TODO
            },
            JobInput::Documents(_docs) => {
                todo!()
            }
            JobInput::DocumentIds(doc_ids) => {
                let jd = &mut self.job_data;
                for ds in doc_ids
                    .iter()
                    .rev()
                    .map(|d| &jd.session_data.documents[*d as usize].source)
                {
                    match ds {
                        DocumentSource::Url(_) => todo!(),
                        DocumentSource::File(_) => todo!(),
                        DocumentSource::Bytes(_) => todo!(),
                        DocumentSource::Stdin => todo!(),
                        DocumentSource::String(str) => {
                            self.job_data.fields[input_data].field_data.push_str(str, 1);
                            entry_count += 1;
                        }
                    }
                }
            }
            JobInput::Stdin => {
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
            let (tf_state, tf_data) = setup_ts_split_as_entry_point(
                self,
                input_data,
                ms_id,
                entry_count,
                job.starting_ops.iter(),
            );
            let tf_id = self.add_transform(tf_state, tf_data);
            self.ready_queue.push(tf_id);
        } else {
            self.setup_transforms_from_op(ms_id, entry_count, job.starting_ops[0], input_data);
        }
        Ok(())
    }
    pub fn new(sess: &'a SessionData) -> Self {
        WorkerThreadSession {
            transform_data: Default::default(),
            job_data: JobData {
                transforms: Default::default(),
                session_data: sess,
                match_sets: Default::default(),
                ready_queue: Default::default(),
                fields: Default::default(),
                stream_producers: Default::default(),
                scratch_memory: Default::default(),
            },
        }
    }

    pub fn remove_transform(&mut self, tf_id: TransformId, triggering_field: FieldId) {
        self.transforms.release(tf_id);
        self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
        let field = &mut self.fields[triggering_field];

        field.ref_count -= 1;
        if field.ref_count == 0 {
            self.remove_field(triggering_field);
        }
    }

    fn handle_split_expansion(&mut self, tf_id: TransformId) {
        // we have to temporarily move the targets out of split so we can modify
        // self while accessing them
        let mut targets;
        if let TransformData::Split(ref mut split) = self.transform_data[usize::from(tf_id)] {
            targets = std::mem::replace(&mut split.targets, Default::default());
        } else {
            debug_assert!(false, "unexpected transform type");
            return;
        }
        for i in 0..targets.len() {
            let op = targets[i];
            let ms_id = self.add_match_set();
            let input_field = self.add_field(ms_id, None);
            targets[i] = self.setup_transforms_from_op(
                ms_id,
                0,
                <NonMaxUsize as TryInto<usize>>::try_into(op).unwrap() as OperatorId,
                input_field,
            );
        }
        if let TransformData::Split(ref mut split) = self.transform_data[usize::from(tf_id)] {
            split.targets = targets;
            handle_tf_split(&mut self.job_data, tf_id, split, false);
        } else {
            debug_assert!(false, "unexpected transform type");
        }
    }

    fn run_batch_mode(&mut self) -> Result<bool, ScrError> {
        while let Some(tf_id) = self.ready_queue.peek().map(|t| *t) {
            match self.transform_data[usize::from(tf_id)] {
                TransformData::Disabled => unreachable!(),
                TransformData::Split(ref mut split) => {
                    if !split.expanded {
                        self.handle_split_expansion(tf_id);
                    } else {
                        handle_tf_split(&mut self.job_data, tf_id, split, false);
                    }
                }
                TransformData::Print => {
                    handle_print_batch_mode(self, tf_id);
                }
                TransformData::Regex(ref mut regex) => {
                    handle_tf_regex_batch_mode(&mut self.job_data, tf_id, regex);
                }
                TransformData::Format(_) => todo!(),
            }
        }

        Ok(true)
    }
    fn run_stream_mode(&mut self) -> Result<bool, ScrError> {
        while let Some(tf_id) = self.ready_queue.peek().map(|t| *t) {
            match self.transform_data[usize::from(tf_id)] {
                TransformData::Disabled => unreachable!(),
                TransformData::Split(ref mut split) => {
                    if !split.expanded {
                        self.handle_split_expansion(tf_id);
                    } else {
                        handle_tf_split(&mut self.job_data, tf_id, split, false);
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
            let tf_data = match op_data {
                OperatorData::Print => TransformData::Print,
                OperatorData::Split(ref split) => TransformData::Split(setup_tf_split(split)),
                OperatorData::Regex(re) => {
                    TransformData::Regex(setup_tf_regex(self, match_set_id, re))
                }
                OperatorData::Format(ref fmt) => {
                    output_field = self.add_field(match_set_id, None);
                    TransformData::Format(setup_tf_format(self, output_field, fmt))
                }
            };
            let tf_state = TransformState {
                available_batch_size: start_tf_id.map(|_| 0).unwrap_or(available_batch_size),
                input_field: prev_field_id,
                match_set_id,
                stream_successor: None,
                stream_producers_slot_index: None,
                desired_batch_size: default_batch_size,
                successor: None,
            };
            let tf_id = if start_tf_id.is_none() {
                let id = self.add_transform(tf_state, tf_data);
                if available_batch_size > 0 {
                    self.ready_queue.push(id);
                }
                start_tf_id = Some(id);

                id
            } else {
                self.add_transform(tf_state, tf_data)
            };
            if let Some(prev_tf) = prev_tf {
                self.transforms[prev_tf].successor = Some(tf_id);
            }
            prev_tf = Some(tf_id);
            prev_field_id = output_field;
        }
        start_tf_id.unwrap()
    }
    pub fn add_transform(&mut self, state: TransformState, data: TransformData<'a>) -> TransformId {
        let id = self.transforms.push(state);
        if self.transform_data.len() < self.transforms.len() {
            self.transform_data
                .resize_with(self.transforms.len(), || TransformData::Disabled);
        }
        self.transform_data[usize::from(id)] = data;
        id
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
