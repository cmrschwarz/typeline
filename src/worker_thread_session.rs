use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
    iter,
    ops::DerefMut,
};

use is_terminal::IsTerminal;
use nonmax::NonMaxUsize;

use crate::{
    chain::BufferingMode,
    context::SessionData,
    document::DocumentSource,
    field_data::{
        fd_iter::{FDIterMut, FDIterator},
        fd_iter_hall::FDIterHall,
        field_command_buffer::FieldCommandBuffer,
        EntryId, FieldData,
    },
    operations::{
        errors::{OperatorApplicationError, OperatorSetupError},
        file_reader::{
            handle_tf_file_reader_batch_mode, handle_tf_file_reader_producer_mode,
            handle_tf_file_reader_stream_mode, setup_tf_file_reader_as_entry_point, FileType,
        },
        format::setup_tf_format,
        operator::{OperatorData, OperatorId},
        print::{handle_tf_print_batch_mode, handle_tf_print_stream_mode, setup_tf_print},
        regex::{handle_tf_regex_batch_mode, setup_tf_regex},
        split::{handle_tf_split, setup_tf_split, setup_ts_split_as_entry_point},
        transform::{TransformData, TransformId, TransformOrderingId, TransformState},
    },
    scr_error::ScrError,
    stream_field_data::StreamFieldData,
    utils::string_store::StringStoreEntry,
    utils::universe::Universe,
    worker_thread::{Job, JobInput},
};

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
    pub field_data: FDIterHall,
    pub stream_field_data: StreamFieldData,
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
    pub command_buffer: FieldCommandBuffer,
    pub field_name_map: HashMap<StringStoreEntry, VecDeque<FieldId>>,
}

pub struct WorkerThreadSession<'a> {
    pub transform_data: Vec<TransformData<'a>>,
    pub job_data: JobData<'a>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct ReadyQueueEntry {
    ord_id: TransformOrderingId,
    tf_id: TransformId,
}

pub struct TransformManager {
    pub transform_ordering_id: TransformOrderingId,
    pub transforms: Universe<TransformId, TransformState>,
    pub ready_queue: BinaryHeap<ReadyQueueEntry>,
    pub stream_producers: VecDeque<TransformId>,
}

pub struct EntryData {
    pub match_sets: Universe<MatchSetId, MatchSet>,
    pub fields: Universe<FieldId, RefCell<Field>>,
}

pub struct JobData<'a> {
    pub session_data: &'a SessionData,

    pub tf_mgr: TransformManager,
    pub entry_data: EntryData,

    pub(crate) ids_temp_buffer: Vec<NonMaxUsize>,
}

pub type EnterStreamModeFlag = bool;
pub type StreamProducerDoneFlag = bool;

impl TransformManager {
    pub fn claim_transform_ordering_id(&mut self) -> TransformOrderingId {
        self.transform_ordering_id = self.transform_ordering_id.checked_add(1).unwrap();
        self.transform_ordering_id
    }
    pub fn claim_batch(&mut self, tf_id: TransformId) -> (usize, FieldId) {
        let tf = &mut self.transforms[tf_id];
        let batch = tf.desired_batch_size.min(tf.available_batch_size);
        tf.available_batch_size -= batch;
        if tf.available_batch_size == 0 {
            let top = self.ready_queue.pop();
            debug_assert!(top.unwrap().tf_id == tf_id);
            tf.is_ready = false;
        }
        (batch, tf.input_field)
    }
    pub fn inform_transform_batch_available(&mut self, tf_id: TransformId, batch_size: usize) {
        let tf = &mut self.transforms[tf_id];
        tf.available_batch_size += batch_size;
        if tf.available_batch_size == batch_size {
            self.push_tf_in_ready_queue(tf_id);
        }
    }
    pub fn inform_successor_batch_available(&mut self, tf_id: TransformId, batch_size: usize) {
        if let Some(succ_tf_id) = self.transforms[tf_id].successor {
            self.inform_transform_batch_available(succ_tf_id, batch_size);
        }
    }
    pub fn push_tf_in_ready_queue(&mut self, tf_id: TransformId) {
        let tf = &mut self.transforms[tf_id];
        if !tf.is_ready {
            tf.is_ready = true;
            self.ready_queue.push(ReadyQueueEntry {
                ord_id: tf.ordering_id,
                tf_id: tf_id,
            });
        }
    }
    pub fn push_successor_in_ready_queue(&mut self, tf_id: TransformId) {
        if let Some(succ_tf_id) = self.transforms[tf_id].successor {
            self.push_tf_in_ready_queue(succ_tf_id);
        }
    }
}

impl EntryData {
    pub fn add_field(&mut self, ms_id: MatchSetId, name: Option<StringStoreEntry>) -> FieldId {
        let field_id = self.fields.claim();
        let mut field = self.fields[field_id as FieldId].borrow_mut();
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
        {
            let mut field = self.fields[id].borrow_mut();
            let match_set = &mut self.match_sets[field.match_set];
            if let Some(ref name) = field.name {
                let refs = match_set.field_name_map.get_mut(name).unwrap();
                debug_assert!(*refs.front().unwrap() == id);
                refs.pop_front();
            }
            field.ref_count = 0;
            field.field_data.clear();
        }
        self.fields.release(id);
    }
    pub fn add_match_set(&mut self) -> MatchSetId {
        let id = self.match_sets.claim();
        id
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    pub fn push_entry_error(&mut self, _ms_id: MatchSetId, _err: OperatorApplicationError) {
        todo!()
    }
}

impl<'a> JobData<'a> {
    pub(crate) fn apply_field_actions(&self, tf_id: TransformId) {
        let mut tf = &self.tf_mgr.transforms[tf_id];
        'transforms_loop: loop {
            let field = &mut self.entry_data.fields[tf.input_field].borrow_mut();
            let mut field_offset: isize = 0;

            //TODO
            let last_field = tf.input_field;
            loop {
                if let Some(prev_tf_id) = self.tf_mgr.transforms[tf_id].predecessor {
                    tf = &self.tf_mgr.transforms[prev_tf_id];
                    if tf.input_field != last_field {
                        break;
                    }
                } else {
                    break 'transforms_loop;
                }
            }
        }
    }
}

impl<'a> WorkerThreadSession<'a> {
    fn setup_job(&mut self, job: Job) -> Result<(), ScrError> {
        self.job_data.entry_data.match_sets.clear();
        self.job_data.entry_data.fields.clear();
        self.job_data.tf_mgr.ready_queue.clear();
        let ms_id = self.job_data.entry_data.add_match_set();
        let input_data = self.job_data.entry_data.add_field(ms_id, None);
        let mut entry_count: usize = 0;
        let mut starting_tfs = std::mem::take(&mut self.job_data.ids_temp_buffer);
        match job.data {
            JobInput::FieldData(_fd) => loop {
                todo!();
            },
            JobInput::Documents(_docs) => {
                todo!();
            }
            JobInput::DocumentIds(doc_ids) => {
                let jd = &mut self.job_data;
                for doc in doc_ids
                    .iter()
                    .rev()
                    .map(|d| &jd.session_data.documents[*d as usize])
                {
                    match &doc.source {
                        DocumentSource::Url(_) => todo!(),
                        DocumentSource::File(path) => match std::fs::File::open(path) {
                            Ok(f) => {
                                let line_buffer = match self.job_data.session_data.chains
                                    [doc.target_chains[0] as usize]
                                    .settings
                                    .buffering_mode
                                {
                                    BufferingMode::LineBuffer => true,
                                    BufferingMode::LineBufferIfTTY => f.is_terminal(),
                                    BufferingMode::BlockBuffer
                                    | BufferingMode::LineBufferStdin
                                    | BufferingMode::LineBufferStdinIfTTY => false,
                                };
                                let file = FileType::File(f);
                                let (state, data) = setup_tf_file_reader_as_entry_point(
                                    &mut self.job_data.tf_mgr,
                                    input_data,
                                    input_data,
                                    ms_id,
                                    1,
                                    file,
                                    line_buffer,
                                );
                                let tf_id = self.add_transform(state, data);
                                self.job_data.tf_mgr.push_tf_in_ready_queue(tf_id);
                                starting_tfs.push(tf_id);
                                break;
                            }
                            Err(err) => {
                                return Err(OperatorSetupError {
                                    message: Cow::Owned(err.to_string()),
                                    op_id: job.starting_ops[0],
                                }
                                .into());
                            }
                        },
                        DocumentSource::Bytes(_) => todo!(),
                        DocumentSource::Stdin => todo!(),
                        DocumentSource::Integer(int) => {
                            self.job_data.entry_data.fields[input_data]
                                .borrow_mut()
                                .field_data
                                .push_int(*int, 1);
                            entry_count += 1;
                        }
                        DocumentSource::String(str) => {
                            self.job_data.entry_data.fields[input_data]
                                .borrow_mut()
                                .field_data
                                .push_str(str, 1, true);
                            entry_count += 1;
                        }
                    }
                }
                if entry_count == 0 && starting_tfs.len() == 0 {
                    return Err(OperatorSetupError::new(
                        "must supply at least one initial data element",
                        job.starting_ops[0],
                    )
                    .into());
                }
            }
            JobInput::Stdin => {
                //TODO: figure out which chain this setting should come from
                let line_buffered =
                    match self.job_data.session_data.chains[0].settings.buffering_mode {
                        BufferingMode::BlockBuffer => false,
                        BufferingMode::LineBuffer | BufferingMode::LineBufferStdin => true,
                        BufferingMode::LineBufferStdinIfTTY | BufferingMode::LineBufferIfTTY => {
                            std::io::stdout().is_terminal()
                        }
                    };
                let (state, data) = setup_tf_file_reader_as_entry_point(
                    &mut self.job_data.tf_mgr,
                    input_data,
                    input_data,
                    ms_id,
                    1,
                    FileType::Stdin(std::io::stdin().lock()),
                    line_buffered,
                );
                let tf_id = self.add_transform(state, data);
                self.job_data.tf_mgr.push_tf_in_ready_queue(tf_id);
                starting_tfs.push(tf_id);
            }
        }
        debug_assert!(!job.starting_ops.is_empty());
        let first_tf = if job.starting_ops.len() > 1 {
            let (tf_state, tf_data) = setup_ts_split_as_entry_point(
                &mut self.job_data,
                input_data,
                ms_id,
                entry_count,
                job.starting_ops.iter(),
            );
            let tf_id = self.add_transform(tf_state, tf_data);
            self.job_data.tf_mgr.push_tf_in_ready_queue(tf_id);
            tf_id
        } else {
            self.setup_transforms_from_op(ms_id, entry_count, job.starting_ops[0], input_data)
        };
        for tf in &starting_tfs {
            self.job_data.tf_mgr.transforms[*tf].successor = Some(first_tf);
        }
        self.job_data.ids_temp_buffer = std::mem::take(&mut starting_tfs);
        Ok(())
    }
    pub fn new(sess: &'a SessionData) -> Self {
        WorkerThreadSession {
            transform_data: Default::default(),
            job_data: JobData {
                session_data: sess,

                tf_mgr: TransformManager {
                    transforms: Default::default(),
                    transform_ordering_id: TransformOrderingId::new(1).unwrap(),
                    ready_queue: Default::default(),
                    stream_producers: Default::default(),
                },
                entry_data: EntryData {
                    fields: Default::default(),
                    match_sets: Default::default(),
                },
                ids_temp_buffer: Default::default(),
                actions_temp_buffer: Default::default(),
            },
        }
    }

    pub fn remove_transform(&mut self, tf_id: TransformId, triggering_field: FieldId) {
        let rc = {
            self.job_data.tf_mgr.transforms.release(tf_id);
            self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
            let mut field = self.job_data.entry_data.fields[triggering_field].borrow_mut();

            field.ref_count -= 1;
            field.ref_count
        };
        if rc == 0 {
            self.job_data.entry_data.remove_field(triggering_field);
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
            let ms_id = self.job_data.entry_data.add_match_set();
            let input_field = self.job_data.entry_data.add_field(ms_id, None);
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
        while let Some(ReadyQueueEntry { ord_id: _, tf_id }) =
            self.job_data.tf_mgr.ready_queue.peek().map(|t| *t)
        {
            let jd = &mut self.job_data;
            match self.transform_data[usize::from(tf_id)] {
                TransformData::Disabled => unreachable!(),
                TransformData::Split(ref mut split) => {
                    if !split.expanded {
                        self.handle_split_expansion(tf_id);
                    } else {
                        handle_tf_split(&mut self.job_data, tf_id, split, false);
                    }
                }
                TransformData::Print(ref mut tf) => {
                    handle_tf_print_batch_mode(jd, tf_id, tf);
                }
                TransformData::Regex(ref mut tf) => {
                    handle_tf_regex_batch_mode(jd, tf_id, tf);
                }
                TransformData::Format(_) => todo!(),
                TransformData::FileReader(ref mut tf) => {
                    if handle_tf_file_reader_batch_mode(jd, tf_id, tf) {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }
    fn run_stream_mode(&mut self) -> Result<(), ScrError> {
        loop {
            while let Some(rqe) = self.job_data.tf_mgr.ready_queue.pop() {
                let ReadyQueueEntry { tf_id, ord_id: _ } = rqe;
                let jd = &mut self.job_data;
                let tf = &mut jd.tf_mgr.transforms[tf_id];
                if tf.is_stream_producer {
                    self.job_data.tf_mgr.ready_queue.push(rqe);
                    break;
                }
                tf.is_ready = false;
                match &mut self.transform_data[usize::from(tf_id)] {
                    TransformData::Disabled => unreachable!(),
                    TransformData::Split(split) => {
                        if !split.expanded {
                            self.handle_split_expansion(tf_id);
                        } else {
                            handle_tf_split(&mut self.job_data, tf_id, split, false);
                        }
                    }
                    TransformData::Print(tf) => {
                        handle_tf_print_stream_mode(jd, tf_id, tf);
                    }
                    TransformData::Regex(_) => todo!(),
                    TransformData::Format(_) => todo!(),
                    TransformData::FileReader(fr) => {
                        handle_tf_file_reader_stream_mode(jd, tf_id, fr);
                    }
                }
            }
            let prod_tf_id;
            if let Some(prod) = self.job_data.tf_mgr.stream_producers.pop_front() {
                prod_tf_id = prod;
            } else {
                return Ok(());
            }
            let jd = &mut self.job_data;
            let done = match self.transform_data[usize::from(prod_tf_id)] {
                TransformData::FileReader(ref mut fr) => {
                    handle_tf_file_reader_producer_mode(jd, prod_tf_id, fr)
                }
                _ => panic!("transform is not a valid producer"),
            };
            if !done {
                self.job_data.tf_mgr.stream_producers.push_back(prod_tf_id);
            }
        }
    }
    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        available_batch_size: usize,
        start_op_id: OperatorId,
        input_field_id: FieldId,
    ) -> TransformId {
        let mut start_tf_id = None;
        let start_op = &self.job_data.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.job_data.session_data.chains[start_op.chain_id as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = None;
        let mut prev_field_id = input_field_id;
        let mut output_field;
        let ops = &self.job_data.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..];
        for op_id in ops {
            let op_data = &self.job_data.session_data.operator_data[*op_id as usize];
            let tf_data;
            let jd = &mut self.job_data;
            (tf_data, output_field) = match op_data {
                OperatorData::Split(ref split) => setup_tf_split(jd, ms_id, input_field_id, split),
                OperatorData::Print => setup_tf_print(jd, ms_id, input_field_id),
                OperatorData::Regex(ref re) => setup_tf_regex(jd, ms_id, input_field_id, re),
                OperatorData::Format(ref fmt) => setup_tf_format(jd, ms_id, input_field_id, fmt),
            };
            let tf_state = TransformState {
                available_batch_size: start_tf_id.map(|_| 0).unwrap_or(available_batch_size),
                input_field: prev_field_id,
                match_set_id: ms_id,
                desired_batch_size: default_batch_size,
                successor: None,
                predecessor: prev_tf,
                op_id: *op_id,
                ordering_id: self.job_data.tf_mgr.claim_transform_ordering_id(),
                is_ready: false,
                is_stream_producer: false,
            };
            let tf_id = if start_tf_id.is_none() {
                let tf_id = self.add_transform(tf_state, tf_data);
                if available_batch_size > 0 {
                    self.job_data.tf_mgr.push_tf_in_ready_queue(tf_id);
                }
                start_tf_id = Some(tf_id);
                tf_id
            } else {
                self.add_transform(tf_state, tf_data)
            };
            if let Some(prev_tf) = prev_tf {
                self.job_data.tf_mgr.transforms[prev_tf].successor = Some(tf_id);
            }
            prev_tf = Some(tf_id);
            prev_field_id = output_field;
        }
        start_tf_id.unwrap()
    }
    pub fn add_transform(&mut self, state: TransformState, data: TransformData<'a>) -> TransformId {
        let id = self.job_data.tf_mgr.transforms.push(state);
        if self.transform_data.len() < self.job_data.tf_mgr.transforms.len() {
            self.transform_data
                .resize_with(self.job_data.tf_mgr.transforms.len(), || {
                    TransformData::Disabled
                });
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
            self.run_stream_mode()?;
        }

        Ok(())
    }
}
