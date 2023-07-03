use std::{
    cell::RefCell,
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
    iter,
};

use nonmax::NonMaxUsize;

use crate::{
    context::SessionData,
    field_data::{
        fd_command_buffer::FDCommandBuffer,
        fd_iter_hall::{FDIterHall, FDIterId},
        fd_push_interface::FDPushInterface,
        EntryId, FieldData,
    },
    operations::{
        data_inserter::{handle_tf_data_inserter, setup_tf_data_inserter},
        errors::OperatorApplicationError,
        file_reader::{handle_tf_file_reader, setup_tf_file_reader},
        format::setup_tf_format,
        operator::{OperatorData, OperatorId},
        print::{handle_tf_print, handle_tf_print_stream_value_update, setup_tf_print},
        regex::{handle_tf_regex, setup_tf_regex},
        split::{handle_tf_split, setup_tf_split, setup_ts_split_as_entry_point},
        string_sink::{
            handle_tf_string_sink, handle_tf_string_sink_stream_value_update, setup_tf_string_sink,
        },
        transform::{TransformData, TransformId, TransformOrderingId, TransformState},
    },
    scr_error::ScrError,
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::string_store::StringStoreEntry,
    utils::universe::Universe,
    worker_thread::Job,
};

pub const FIELD_REF_LOOKUP_ITER_ID: FDIterId = 0 as FDIterId;
pub const ERROR_FIELD_PSEUDO_STR: usize = 0;

#[derive(Default)]
pub struct Field {
    // number of tfs that might read this field
    // if this drops to zero, remove the field
    // TODO:
    // (unless this is named, we have a fwd, and this is not shadowed before it)
    pub ref_count: usize,
    pub match_set: MatchSetId,
    pub shadowed_after_tf: TransformId,
    pub producing_transform_action_set_id: usize,
    pub last_applied_action_set_id: usize,

    pub name: Option<StringStoreEntry>,
    pub working_set_idx: Option<NonMaxUsize>,
    pub field_data: FDIterHall,
}

pub type FieldId = NonMaxUsize;

pub type MatchSetId = NonMaxUsize;

#[repr(C)]
pub struct MatchSet {
    pub stream_batch_size: usize,
    pub stream_participants: Vec<TransformId>,
    pub working_set_updates: Vec<(EntryId, FieldId)>,
    //should not contain tf input fields (?)
    pub working_set: Vec<FieldId>,
    pub command_buffer: FDCommandBuffer,
    pub field_name_map: HashMap<StringStoreEntry, VecDeque<FieldId>>,
    pub err_field_id: FieldId,
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

pub struct RecordManager {
    pub match_sets: Universe<MatchSetId, MatchSet>,
    pub fields: Universe<FieldId, RefCell<Field>>,
}

pub struct StreamValueUpdate {
    sv_id: StreamValueId,
    tf_id: TransformId,
    custom: usize,
}

#[derive(Default)]
pub struct StreamValueManager {
    pub stream_values: Universe<StreamValueId, StreamValue>,
    pub updates: VecDeque<StreamValueUpdate>,
}

pub struct JobData<'a> {
    pub session_data: &'a SessionData,
    pub tf_mgr: TransformManager,
    pub record_mgr: RecordManager,
    pub sv_mgr: StreamValueManager,
}

impl TransformManager {
    pub fn claim_transform_ordering_id(&mut self) -> TransformOrderingId {
        let res = self.transform_ordering_id;
        self.transform_ordering_id = self.transform_ordering_id.checked_add(1).unwrap();
        res
    }
    pub fn inform_transform_batch_available(&mut self, tf_id: TransformId, batch_size: usize) {
        let tf = &mut self.transforms[tf_id];
        tf.available_batch_size += batch_size;
        if tf.available_batch_size > 0 && !tf.is_ready {
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
    pub fn make_stream_producer(&mut self, tf_id: TransformId) {
        let tf = &mut self.transforms[tf_id];
        tf.is_stream_producer = true;
        self.stream_producers.push_back(tf_id);
    }
    pub fn unlink_transform(&mut self, tf_id: TransformId, available_batch_for_successor: usize) {
        let tf = &self.transforms[tf_id];
        let predecessor = tf.predecessor;
        let bs = tf.available_batch_size;
        let successor = tf.successor;

        if let Some(pred_id) = predecessor {
            self.transforms[pred_id].successor = successor;
        }
        if let Some(succ_id) = successor {
            self.transforms[succ_id].predecessor = predecessor;
            self.inform_transform_batch_available(succ_id, bs + available_batch_for_successor);
        }
    }
    pub fn update_ready_state(&mut self, tf_id: TransformId) {
        let tf = &self.transforms[tf_id];
        debug_assert!(!tf.is_ready);
        if tf.available_batch_size > 0 {
            self.push_tf_in_ready_queue(tf_id);
        }
    }
}

impl RecordManager {
    fn setup_field(
        &mut self,
        field_id: FieldId,
        ms_id: MatchSetId,
        name: Option<StringStoreEntry>,
    ) {
        let mut field = self.fields[field_id as FieldId].borrow_mut();
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
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
    }
    pub fn add_field(&mut self, ms_id: MatchSetId, name: Option<StringStoreEntry>) -> FieldId {
        let field_id = self.fields.claim();
        self.setup_field(field_id, ms_id, name);
        field_id
    }
    pub fn add_field_with_data(
        &mut self,
        ms_id: MatchSetId,
        name: Option<StringStoreEntry>,
        data: FieldData,
    ) -> FieldId {
        let field_id = if self.fields.has_unclaimed_entries() {
            let id = self.fields.claim();
            self.fields[id]
                .borrow_mut()
                .field_data
                .reset_with_data(data);
            id
        } else {
            self.fields.claim_with(|| {
                RefCell::new(Field {
                    field_data: FDIterHall::new_with_data(data),
                    ..Default::default()
                })
            })
        };
        self.setup_field(field_id, ms_id, name);
        field_id
    }
    pub fn remove_field(&mut self, id: FieldId) {
        let mut field = self.fields[id].borrow_mut();
        let match_set = &mut self.match_sets[field.match_set];
        if let Some(ref name) = field.name {
            let refs = match_set.field_name_map.get_mut(name).unwrap();
            debug_assert!(*refs.front().unwrap() == id);
            refs.pop_front();
        }
        field.ref_count = 0;
        field.field_data.reset();
        drop(field);
        self.fields.release(id);
    }
    pub fn add_match_set(&mut self) -> MatchSetId {
        let id = self.match_sets.peek_claim_id();
        let ef = self.add_field(id, None);
        self.match_sets.claim_with(|| MatchSet {
            stream_batch_size: 0,
            stream_participants: Default::default(),
            working_set_updates: Default::default(),
            working_set: Vec::new(),
            command_buffer: Default::default(),
            field_name_map: Default::default(),
            err_field_id: ef,
        })
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    pub fn push_entry_error(
        &self,
        ms_id: MatchSetId,
        field_pos: usize,
        err: OperatorApplicationError,
        run_length: usize,
    ) {
        let ms = &self.match_sets[ms_id];
        let mut ef = self.fields[ms.err_field_id].borrow_mut();
        let fd = &mut ef.field_data;
        let last_pos = fd.field_count() + fd.field_index_offset();
        debug_assert!(last_pos <= field_pos);
        fd.push_unset(field_pos - last_pos, false);
        fd.push_error(err, run_length, true, true);
        return;
    }
    // this is usually called while iterating over an input field that contains field references
    // we therefore do NOT want to require a mutable reference over the field data, because that forces the caller to kill their iterator
    // instead we `split up` this struct to only require a mutable reference for the MatchSets, which we need to modify the command buffer
    pub fn apply_field_commands(
        fields: &Universe<FieldId, RefCell<Field>>,
        match_sets: &mut Universe<MatchSetId, MatchSet>,
        field: FieldId,
    ) {
        let mut f = fields[field].borrow_mut();
        let match_set = f.match_set;
        let cb = &mut match_sets[match_set].command_buffer;
        let last_acs = cb.last_action_set_id();
        let last_applied_acs = f.last_applied_action_set_id;
        if last_applied_acs < last_acs {
            f.last_applied_action_set_id = last_acs;
            cb.execute([f].into_iter(), last_applied_acs, last_acs);
        }
    }
}

impl StreamValueManager {
    pub fn inform_stream_value_subscribers(&mut self, sv_id: StreamValueId, done: bool) {
        let sv = &self.stream_values[sv_id];
        for sub in &sv.subscribers {
            if !sub.notify_only_once_done || done {
                self.updates.push_back(StreamValueUpdate {
                    sv_id: sv_id,
                    tf_id: sub.tf_id,
                    custom: sub.custom_data,
                });
            }
        }
    }
    pub fn drop_field_value_subscription(
        &mut self,
        sv_id: StreamValueId,
        tf_id_to_remove: Option<TransformId>,
    ) {
        let sv = &mut self.stream_values[sv_id];
        sv.ref_count -= 1;
        if sv.ref_count == 0 {
            sv.data = StreamValueData::Dropped;
            self.stream_values.release(sv_id);
        } else if let Some(tf_id) = tf_id_to_remove {
            sv.subscribers.swap_remove(
                sv.subscribers
                    .iter()
                    .position(|sub| sub.tf_id == tf_id)
                    .unwrap(),
            );
        }
    }
}

impl JobData<'_> {
    pub fn claim_batch(
        &mut self,
        tf_id: TransformId,
        output_fields: &[FieldId],
    ) -> (usize, FieldId) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        let tf_ord_id = usize::from(tf.ordering_id);
        let cb = &mut self.record_mgr.match_sets[tf.match_set_id].command_buffer;
        let max_action_set_id = cb.last_action_set_id();
        let mut regular_command_application_needed = false;
        let mut custom_command_application_needed = false;
        const CLEARED_MARKER_IDX: usize = 0;
        const REGULAR_MARKER_IDX: usize = 1;
        const CUSTOM_MARKER_IDX: usize = 2;
        if tf.last_consumed_batch_size > 0 {
            for ofid in output_fields {
                let mut f = self.record_mgr.fields[*ofid].borrow_mut();
                if f.field_data.field_count() == tf.last_consumed_batch_size {
                    f.field_data.clear();
                    f.producing_transform_action_set_id = CLEARED_MARKER_IDX;
                    f.last_applied_action_set_id = tf_ord_id;
                } else {
                    if f.last_applied_action_set_id == tf_ord_id {
                        f.producing_transform_action_set_id = REGULAR_MARKER_IDX;
                        regular_command_application_needed = true;
                    } else if f.last_applied_action_set_id != max_action_set_id {
                        f.producing_transform_action_set_id = CUSTOM_MARKER_IDX;
                        custom_command_application_needed = true;
                    }
                }
            }
        }

        if regular_command_application_needed && tf_ord_id < max_action_set_id {
            let iter = output_fields
                .iter()
                .map(|fid| self.record_mgr.fields[*fid].borrow_mut())
                .filter(|fid| fid.producing_transform_action_set_id == REGULAR_MARKER_IDX);
            cb.execute(iter, tf_ord_id + 1, max_action_set_id);
        }
        if custom_command_application_needed {
            let iter = output_fields
                .iter()
                .map(|fid| self.record_mgr.fields[*fid].borrow_mut())
                .filter(|fid| fid.producing_transform_action_set_id == CUSTOM_MARKER_IDX);
            for f in iter {
                let min_idx = f.last_applied_action_set_id;
                cb.execute(iter::once(f), min_idx + 1, max_action_set_id);
            }
        }
        //TODO: this is incorrect. merge instead
        cb.erase_action_sets(tf_ord_id + 1);
        let batch = tf.desired_batch_size.min(tf.available_batch_size);
        tf.available_batch_size -= batch;
        tf.last_consumed_batch_size = batch;
        (batch, tf.input_field)
    }
}

impl<'a> WorkerThreadSession<'a> {
    fn setup_job(&mut self, mut job: Job) -> Result<(), ScrError> {
        self.job_data.record_mgr.match_sets.clear();
        self.job_data.record_mgr.fields.clear();
        self.job_data.tf_mgr.ready_queue.clear();
        let ms_id = self.job_data.record_mgr.add_match_set();
        //TODO: unpack record set properly here
        let input_record_count = job.data.adjust_field_lengths();
        let mut input_data = None;
        for fd in job.data.fields.into_iter() {
            let field_id = self
                .job_data
                .record_mgr
                .add_field_with_data(ms_id, fd.name, fd.data);
            if input_data.is_none() {
                input_data = Some(field_id);
            }
            //TODO: add all to working set?
        }
        let input_data =
            input_data.unwrap_or_else(|| self.job_data.record_mgr.add_field(ms_id, None));

        debug_assert!(!job.starting_ops.is_empty());
        let first_tf_id = if job.starting_ops.len() > 1 {
            let (tf_state, tf_data) = setup_ts_split_as_entry_point(
                &mut self.job_data,
                input_data,
                ms_id,
                input_record_count,
                job.starting_ops.iter(),
            );
            let tf_id = self.add_transform(tf_state, tf_data);
            self.job_data.tf_mgr.push_tf_in_ready_queue(tf_id);
            tf_id
        } else {
            self.setup_transforms_from_op(
                ms_id,
                input_record_count,
                job.starting_ops[0],
                input_data,
            )
        };
        if input_record_count == 0 {
            self.job_data.tf_mgr.push_tf_in_ready_queue(first_tf_id);
        }
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
                record_mgr: RecordManager {
                    fields: Default::default(),
                    match_sets: Default::default(),
                },
                sv_mgr: Default::default(),
            },
        }
    }

    pub fn remove_transform(&mut self, tf_id: TransformId, triggering_field: FieldId) {
        let rc = {
            self.job_data.tf_mgr.transforms.release(tf_id);
            self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
            let mut field = self.job_data.record_mgr.fields[triggering_field].borrow_mut();

            field.ref_count -= 1;
            field.ref_count
        };
        if rc == 0 {
            self.job_data.record_mgr.remove_field(triggering_field);
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
            let ms_id = self.job_data.record_mgr.add_match_set();
            let input_field = self.job_data.record_mgr.add_field(ms_id, None);
            targets[i] = self.setup_transforms_from_op(
                ms_id,
                0,
                <NonMaxUsize as TryInto<usize>>::try_into(op).unwrap() as OperatorId,
                input_field,
            );
        }
        if let TransformData::Split(ref mut split) = self.transform_data[usize::from(tf_id)] {
            split.targets = targets;
            handle_tf_split(&mut self.job_data, tf_id, split);
        } else {
            debug_assert!(false, "unexpected transform type");
        }
    }

    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        available_batch_size: usize,
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
    ) -> TransformId {
        let mut start_tf_id = None;
        let start_op = &self.job_data.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.job_data.session_data.chains[start_op.chain_id as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = None;
        let mut prev_field_id = chain_input_field_id;
        let mut output_field;
        let ops = &self.job_data.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..];
        for op_id in ops {
            let op_data = &self.job_data.session_data.operator_data[*op_id as usize];
            let tf_data;
            let jd = &mut self.job_data;
            let mut tf_state = TransformState {
                available_batch_size: start_tf_id.map(|_| 0).unwrap_or(available_batch_size),
                input_field: prev_field_id,
                match_set_id: ms_id,
                desired_batch_size: default_batch_size,
                successor: None,
                predecessor: prev_tf,
                op_id: *op_id,
                ordering_id: jd.tf_mgr.claim_transform_ordering_id(),
                is_ready: false,
                last_consumed_batch_size: 0,
                is_stream_producer: false,
                is_stream_subscriber: false,
            };
            (tf_data, output_field) = match &op_data {
                OperatorData::Split(split) => setup_tf_split(jd, split, &mut tf_state),
                OperatorData::Print => setup_tf_print(jd, &mut tf_state),
                OperatorData::Regex(op) => setup_tf_regex(jd, op, &mut tf_state),
                OperatorData::Format(op) => setup_tf_format(jd, op, &mut tf_state),
                OperatorData::StringSink(op) => setup_tf_string_sink(jd, op, &mut tf_state),
                OperatorData::FileReader(op) => setup_tf_file_reader(jd, op, &mut tf_state),
                OperatorData::DataInserter(op) => setup_tf_data_inserter(jd, op, &mut tf_state),
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
            if let Some(svu) = self.job_data.sv_mgr.updates.pop_back() {
                match &mut self.transform_data[usize::from(svu.tf_id)] {
                    TransformData::Print(tf) => handle_tf_print_stream_value_update(
                        &mut self.job_data,
                        svu.tf_id,
                        tf,
                        svu.sv_id,
                        svu.custom,
                    ),
                    TransformData::StringSink(tf) => handle_tf_string_sink_stream_value_update(
                        &mut self.job_data,
                        svu.tf_id,
                        tf,
                        svu.sv_id,
                        svu.custom,
                    ),
                    TransformData::Split(_) => todo!(),
                    TransformData::Regex(_) => todo!(),
                    TransformData::Format(_) => todo!(),
                    TransformData::FileReader(_) => unreachable!(),
                    TransformData::Disabled => unreachable!(),
                    TransformData::DataInserter(_) => unreachable!(),
                }
                continue;
            }
            if let Some(rqe) = self.job_data.tf_mgr.ready_queue.pop() {
                let ReadyQueueEntry {
                    mut tf_id,
                    ord_id: _,
                } = rqe;

                let mut tf = &mut self.job_data.tf_mgr.transforms[tf_id];
                if tf.is_stream_producer {
                    tf_id = self.job_data.tf_mgr.stream_producers.pop_front().unwrap();
                    tf = &mut self.job_data.tf_mgr.transforms[tf_id];
                    tf.is_stream_producer = false;
                }
                tf.is_ready = false;
                let jd = &mut self.job_data;
                match &mut self.transform_data[usize::from(tf_id)] {
                    TransformData::Split(split) => {
                        if !split.expanded {
                            self.handle_split_expansion(tf_id);
                        } else {
                            handle_tf_split(&mut self.job_data, tf_id, split);
                        }
                    }
                    TransformData::Print(tf) => handle_tf_print(jd, tf_id, tf),
                    TransformData::Regex(tf) => handle_tf_regex(jd, tf_id, tf),
                    TransformData::StringSink(tf) => handle_tf_string_sink(jd, tf_id, tf),
                    TransformData::Format(_tf) => todo!(),
                    TransformData::FileReader(tf) => handle_tf_file_reader(jd, tf_id, tf),
                    TransformData::DataInserter(tf) => handle_tf_data_inserter(jd, tf_id, tf),
                    TransformData::Disabled => unreachable!(),
                }
                continue;
            }
            break;
        }
        //TODO: inspect errors field?
        Ok(())
    }
}
