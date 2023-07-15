use std::{
    cell::{RefCell, RefMut},
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
};

use nonmax::NonMaxUsize;
use smallvec::SmallVec;

use crate::{
    context::SessionData,
    field_data::{
        command_buffer::{ActionListIndex, ActionProducingFieldIndex, CommandBuffer},
        iter_hall::{IterHall, IterId},
        EntryId, FieldData,
    },
    operations::{
        data_inserter::{handle_tf_data_inserter, setup_tf_data_inserter},
        errors::TransformSetupError,
        file_reader::{handle_tf_file_reader, setup_tf_file_reader},
        format::{handle_tf_format, handle_tf_format_stream_value_update, setup_tf_format},
        operator::{OperatorData, OperatorId},
        print::{handle_tf_print, handle_tf_print_stream_value_update, setup_tf_print},
        regex::{handle_tf_regex, handle_tf_regex_stream_value_update, setup_tf_regex},
        sequence::{handle_tf_sequence, setup_tf_sequence},
        split::{handle_tf_split, setup_tf_split, setup_ts_split_as_entry_point},
        string_sink::{
            handle_tf_string_sink, handle_tf_string_sink_stream_value_update, setup_tf_string_sink,
        },
        terminator::{handle_tf_terminator, setup_tf_terminator},
        transform::{TransformData, TransformId, TransformOrderingId, TransformState},
    },
    scr_error::ScrError,
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::string_store::StringStoreEntry,
    utils::universe::Universe,
    worker_thread::Job,
};

pub const FIELD_REF_LOOKUP_ITER_ID: IterId = 0 as IterId;
pub const ERROR_FIELD_PSEUDO_STR: usize = 0;

#[derive(Default)]
pub struct Field {
    pub ref_count: usize,
    pub match_set: MatchSetId,
    pub added_as_placeholder_by_tf: Option<TransformId>,

    pub min_apf_idx: Option<ActionProducingFieldIndex>,
    pub curr_apf_idx: Option<ActionProducingFieldIndex>,
    pub first_unapplied_al: ActionListIndex,

    pub name: Option<StringStoreEntry>,
    pub prev_same_name: Option<FieldId>,
    pub next_same_name: Option<FieldId>,

    pub working_set_idx: Option<NonMaxUsize>,
    pub field_data: IterHall,
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
    pub command_buffer: CommandBuffer,
    pub field_name_map: HashMap<StringStoreEntry, FieldId>,
}

impl MatchSet {}

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
    pub fn claim_batch(&mut self, tf_id: TransformId) -> (usize, bool) {
        let tf = &mut self.transforms[tf_id];
        let batch_size = tf.desired_batch_size.min(tf.available_batch_size);
        tf.available_batch_size -= batch_size;
        let input_done = tf.input_is_done && tf.available_batch_size == 0;
        (batch_size, input_done)
    }
    pub fn unclaim_batch_size(&mut self, tf_id: TransformId, batch_size: usize) {
        self.transforms[tf_id].available_batch_size += batch_size;
    }
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
    pub fn update_ready_state(&mut self, tf_id: TransformId) {
        let tf = &self.transforms[tf_id];
        debug_assert!(!tf.is_ready);
        if tf.available_batch_size > 0 {
            self.push_tf_in_ready_queue(tf_id);
        }
    }
}

impl RecordManager {
    pub fn get_min_apf_idx(&self, field_id: FieldId) -> Option<ActionProducingFieldIndex> {
        let field = self.fields[field_id].borrow();
        field.min_apf_idx
    }
    fn setup_field(
        &mut self,
        field_id: FieldId,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
        name: Option<StringStoreEntry>,
    ) -> FieldId {
        let mut field = self.fields[field_id as FieldId].borrow_mut();
        field.field_data.reset();
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        field.ref_count = 1;
        field.match_set = ms_id;
        field.added_as_placeholder_by_tf = None;
        field.working_set_idx = None;
        field.min_apf_idx = min_apf;
        drop(field);
        self.set_field_name(field_id, name);
        field_id
    }
    pub fn try_set_field_name(
        &mut self,
        field_id: FieldId,
        name: StringStoreEntry,
    ) -> Result<(), StringStoreEntry> {
        let field = self.fields[field_id].borrow();
        if let Some(curr_name) = field.name {
            if curr_name == name {
                return Ok(());
            }
            return Err(curr_name);
        }
        drop(field);
        self.set_field_name(field_id, Some(name));
        Ok(())
    }
    pub fn set_field_name(&mut self, field_id: FieldId, name: Option<StringStoreEntry>) {
        let mut field = self.fields[field_id].borrow_mut();
        if field.name == name {
            return;
        }
        if let Some(curr_name) = field.name {
            if let Some(next) = field.next_same_name {
                self.fields[next].borrow_mut().prev_same_name = field.prev_same_name;
            } else {
                let fnm = &mut self.match_sets[field.match_set].field_name_map;
                if let Some(prev) = field.prev_same_name {
                    fnm.insert(curr_name, prev);
                } else {
                    fnm.remove(&curr_name);
                }
            }
            if let Some(prev) = field.prev_same_name {
                self.fields[prev].borrow_mut().next_same_name = field.next_same_name;
            }
        }
        if let Some(name) = name {
            match self.match_sets[field.match_set].field_name_map.entry(name) {
                hash_map::Entry::Occupied(mut e) => {
                    let prev = e.insert(field_id);
                    field.prev_same_name = Some(prev);
                    self.fields[prev].borrow_mut().next_same_name = Some(field_id);
                    field.next_same_name = None;
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(field_id);
                    field.prev_same_name = None;
                    field.next_same_name = None;
                }
            }
        }
    }
    pub fn add_field(
        &mut self,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
        name: Option<StringStoreEntry>,
    ) -> FieldId {
        let id = self.fields.claim();
        self.setup_field(id, ms_id, min_apf, name)
    }
    pub fn add_field_with_data(
        &mut self,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
        name: Option<StringStoreEntry>,
        data: FieldData,
    ) -> FieldId {
        let id = self.add_field(ms_id, min_apf, name);
        self.fields[id]
            .borrow_mut()
            .field_data
            .reset_with_data(data);
        id
    }
    pub fn remove_field(&mut self, id: FieldId) {
        self.set_field_name(id, None);
        let field = self.fields[id].borrow_mut();
        #[cfg(feature = "debug_logging")]
        println!("removing field id {id}");
        drop(field);
        self.fields.release(id);
    }
    pub fn add_match_set(&mut self) -> MatchSetId {
        self.match_sets.claim_with(|| MatchSet {
            stream_batch_size: 0,
            stream_participants: Default::default(),
            working_set_updates: Default::default(),
            working_set: Vec::new(),
            command_buffer: Default::default(),
            field_name_map: Default::default(),
        })
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
    // this is usually called while iterating over an input field that contains field references
    // we therefore do NOT want to require a mutable reference over the field data, because that forces the caller to kill their iterator
    // instead we `split up` this struct to only require a mutable reference for the MatchSets, which we need to modify the command buffer
    pub fn apply_field_actions(
        fields: &Universe<FieldId, RefCell<Field>>,
        match_sets: &mut Universe<MatchSetId, MatchSet>,
        field: FieldId,
    ) {
        let mut f = fields[field].borrow_mut();
        let match_set = f.match_set;
        let cb = &mut match_sets[match_set].command_buffer;
        cb.execute_for_field(&mut f);
    }
    pub fn bump_field_refcount(&self, field_id: FieldId) {
        self.fields[field_id].borrow_mut().ref_count += 1;
    }
    pub fn drop_field_refcount(&mut self, field_id: FieldId) {
        let mut field = self.fields[field_id].borrow_mut();
        field.ref_count -= 1;
        if field.ref_count == 0 {
            drop(field);
            self.remove_field(field_id);
        }
    }
}

impl StreamValueManager {
    pub fn inform_stream_value_subscribers(&mut self, sv_id: StreamValueId) {
        let sv = &self.stream_values[sv_id];
        for sub in &sv.subscribers {
            if !sub.notify_only_once_done || sv.done {
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
    pub fn prepare_for_output(&mut self, tf_id: TransformId, output_fields: &[FieldId]) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        if tf.is_appending {
            tf.is_appending = false;
        } else {
            let cb = &mut self.record_mgr.match_sets[tf.match_set_id].command_buffer;
            for ofid in output_fields {
                let mut f = self.record_mgr.fields[*ofid].borrow_mut();
                cb.clear_field_dropping_commands(&mut f);
            }
        }
    }
    pub fn prepare_output_field<'a>(&'a mut self, tf_id: TransformId) -> RefMut<'a, Field> {
        let output_field_id = self.tf_mgr.transforms[tf_id].output_field;
        self.prepare_for_output(tf_id, &[output_field_id]);
        self.record_mgr.fields[output_field_id].borrow_mut()
    }
    pub fn unlink_transform(&mut self, tf_id: TransformId, available_batch_for_successor: usize) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        tf.mark_for_removal = true;
        let predecessor = tf.predecessor;
        let successor = tf.successor;
        let continuation = tf.continuation;
        let input_is_done = tf.input_is_done;

        if let Some(cont_id) = continuation {
            let cont = &mut self.tf_mgr.transforms[cont_id];
            cont.input_is_done = input_is_done;
            cont.successor = successor;
            cont.predecessor = predecessor;
            if let Some(pred_id) = predecessor {
                self.tf_mgr.transforms[pred_id].successor = continuation;
            }
            if let Some(succ_id) = successor {
                let succ = &mut self.tf_mgr.transforms[succ_id];
                succ.input_is_done = true;
                succ.predecessor = continuation;
                succ.available_batch_size += available_batch_for_successor;
                if succ.available_batch_size >= succ.desired_batch_size {
                    self.tf_mgr.push_tf_in_ready_queue(succ_id);
                    self.tf_mgr.transforms[cont_id].is_appending = false;
                }
            }
            self.tf_mgr.push_tf_in_ready_queue(cont_id);
            return;
        }

        if let Some(succ_id) = successor {
            let succ = &mut self.tf_mgr.transforms[succ_id];
            succ.predecessor = predecessor;
            succ.input_is_done = true;
            self.tf_mgr
                .inform_transform_batch_available(succ_id, available_batch_for_successor);
        }
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
        let mut input_data_fields = SmallVec::<[FieldId; 4]>::new();
        for fd in job.data.fields.into_iter() {
            let field_id = self
                .job_data
                .record_mgr
                .add_field_with_data(ms_id, None, fd.name, fd.data);
            input_data_fields.push(field_id);
            if input_data.is_none() {
                input_data = Some(field_id);
            }
        }
        let input_data = input_data.unwrap_or_else(|| {
            let field_id = self.job_data.record_mgr.add_field(ms_id, None, None);
            input_data_fields.push(field_id);
            field_id
        });

        debug_assert!(!job.starting_ops.is_empty());
        if job.starting_ops.len() > 1 {
            let (tf_state, tf_data) = setup_ts_split_as_entry_point(
                &mut self.job_data,
                input_data,
                ms_id,
                input_record_count,
                job.starting_ops.iter(),
            );
            let start_tf_id = self.add_transform(tf_state, tf_data);
            self.job_data.tf_mgr.push_tf_in_ready_queue(start_tf_id);
        } else {
            let start_tf_id =
                self.setup_transforms_from_op(ms_id, job.starting_ops[0], input_data)?;

            let tf = &mut self.job_data.tf_mgr.transforms[start_tf_id];
            tf.input_is_done = true;
            if tf.is_appending {
                if let Some(succ) = tf.successor {
                    let tf_succ = &mut self.job_data.tf_mgr.transforms[succ];
                    tf_succ.available_batch_size = input_record_count;
                    if tf_succ.desired_batch_size <= input_record_count {
                        self.job_data.tf_mgr.transforms[start_tf_id].is_appending = false;
                        self.job_data.tf_mgr.push_tf_in_ready_queue(succ);
                    }
                }
            } else {
                tf.available_batch_size = input_record_count;
            }

            self.job_data.tf_mgr.push_tf_in_ready_queue(start_tf_id);
        }
        for input_field_id in input_data_fields {
            self.job_data.record_mgr.drop_field_refcount(input_field_id);
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

    pub fn remove_transform(&mut self, tf_id: TransformId) {
        let tf = &self.job_data.tf_mgr.transforms[tf_id];
        self.job_data.record_mgr.drop_field_refcount(tf.input_field);
        self.job_data
            .record_mgr
            .drop_field_refcount(tf.output_field);
        #[cfg(feature = "debug_logging")]
        {
            let opname = if let Some(op_id) = tf.op_id {
                self.job_data
                    .session_data
                    .string_store
                    .lookup(self.job_data.session_data.operator_bases[op_id as usize].argname)
            } else {
                "<unknown op>"
            };
            println!("removing transform id {tf_id}: {opname}");
        }
        self.job_data.tf_mgr.transforms.release(tf_id);
        self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
    }

    fn handle_split_expansion(&mut self, tf_id: TransformId) -> Result<(), TransformSetupError> {
        // we have to temporarily move the targets out of split so we can modify
        // self while accessing them
        let mut targets;
        if let TransformData::Split(ref mut split) = self.transform_data[usize::from(tf_id)] {
            targets = std::mem::replace(&mut split.targets, Default::default());
        } else {
            panic!("unexpected transform type");
        }
        let mut res = Ok(());
        for i in 0..targets.len() {
            let op = targets[i];
            let ms_id = self.job_data.record_mgr.add_match_set();
            let input_field = self.job_data.record_mgr.add_field(ms_id, None, None);
            let tf = self.setup_transforms_from_op(
                ms_id,
                <TransformId as TryInto<usize>>::try_into(op).unwrap() as OperatorId,
                input_field,
            );
            match tf {
                Ok(tf) => targets[i] = tf,
                Err(err) => {
                    res = Err(err);
                    break;
                }
            }
        }
        if let TransformData::Split(ref mut split) = self.transform_data[usize::from(tf_id)] {
            split.targets = targets;
            handle_tf_split(&mut self.job_data, tf_id, split);
        } else {
            unreachable!();
        }
        return res;
    }

    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
    ) -> Result<TransformId, TransformSetupError> {
        let mut start_tf_id = None;
        let start_op = &self.job_data.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.job_data.session_data.chains[start_op.chain_id as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = None;
        let mut predecessor_tf = None;
        let mut next_input_field = chain_input_field_id;
        let mut prev_output_field = chain_input_field_id;
        let ops = &self.job_data.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..];
        let mut mark_prev_field_as_placeholder = false;
        for op_id in ops {
            let op_base = &self.job_data.session_data.operator_bases[*op_id as usize];
            let op_data = &self.job_data.session_data.operator_data[*op_id as usize];
            let jd = &mut self.job_data;
            match op_data {
                OperatorData::Key(op) => {
                    assert!(op_base.label.is_none()); //TODO
                    jd.record_mgr
                        .try_set_field_name(prev_output_field, op.key_interned)
                        .map_err(|lbl| {
                            TransformSetupError::new_s(
                                *op_id,
                                format!(
                                    "cannot give key '{}' to field: already named '{}'",
                                    jd.session_data.string_store.lookup(lbl),
                                    jd.session_data.string_store.lookup(lbl)
                                ),
                            )
                        })?;
                    continue;
                }
                OperatorData::Select(op) => {
                    if let Some(field_id) = jd.record_mgr.match_sets[ms_id]
                        .field_name_map
                        .get(&op.key_interned)
                        .cloned()
                    {
                        next_input_field = field_id;
                    } else {
                        let field_id = jd.record_mgr.add_field(
                            ms_id,
                            jd.record_mgr.get_min_apf_idx(next_input_field),
                            Some(op.key_interned),
                        );
                        next_input_field = field_id;
                        //TODO: think about field refcounting
                        mark_prev_field_as_placeholder = true;
                    }
                    continue;
                }
                _ => (),
            }
            let mut output_field = if op_base.append_mode {
                if let Some(lbl) = op_base.label {
                    jd.record_mgr
                        .try_set_field_name(prev_output_field, lbl)
                        .map_err(|lbl| {
                            TransformSetupError::new_s(
                                *op_id,
                                format!(
                                    "cannot give label '{}' to field: already named '{}'",
                                    jd.session_data.string_store.lookup(lbl),
                                    jd.session_data.string_store.lookup(lbl)
                                ),
                            )
                        })?;
                }
                jd.record_mgr.bump_field_refcount(prev_output_field);
                prev_output_field
            } else {
                let min_apf = jd.record_mgr.get_min_apf_idx(prev_output_field);
                jd.record_mgr.add_field(ms_id, min_apf, op_base.label)
            };

            jd.record_mgr.bump_field_refcount(next_input_field);
            let mut tf_state = TransformState::new(
                next_input_field,
                output_field,
                ms_id,
                default_batch_size,
                predecessor_tf,
                Some(*op_id),
                jd.tf_mgr.claim_transform_ordering_id(),
            );
            tf_state.is_appending = op_base.append_mode;

            let tf_id_peek = jd.tf_mgr.transforms.peek_claim_id();
            if mark_prev_field_as_placeholder {
                let mut f = jd.record_mgr.fields[next_input_field].borrow_mut();
                f.added_as_placeholder_by_tf = Some(tf_id_peek);
                mark_prev_field_as_placeholder = false;
            }
            let b = op_base;
            let tf_data = match &op_data {
                OperatorData::Split(op) => setup_tf_split(jd, b, op, &mut tf_state),
                OperatorData::Print(op) => setup_tf_print(jd, b, op, &mut tf_state),
                OperatorData::Regex(op) => setup_tf_regex(jd, b, op, &mut tf_state)?,
                OperatorData::Format(op) => setup_tf_format(jd, b, op, tf_id_peek, &mut tf_state),
                OperatorData::StringSink(op) => setup_tf_string_sink(jd, b, op, &mut tf_state),
                OperatorData::FileReader(op) => setup_tf_file_reader(jd, b, op, &mut tf_state),
                OperatorData::DataInserter(op) => {
                    setup_tf_data_inserter(jd, op_base, op, &mut tf_state)
                }
                OperatorData::Sequence(op) => setup_tf_sequence(jd, op_base, op, &mut tf_state),
                OperatorData::Key(_) => unreachable!(),
                OperatorData::Select(_) => unreachable!(),
            };
            output_field = tf_state.output_field;
            let appending = tf_state.is_appending;
            let tf_id = self.add_transform(tf_state, tf_data);
            debug_assert!(tf_id_peek == tf_id);

            if appending {
                if let Some(prev) = prev_tf {
                    self.job_data.tf_mgr.transforms[prev].continuation = Some(tf_id);
                }
            } else {
                if let Some(pred) = predecessor_tf {
                    self.job_data.tf_mgr.transforms[pred].successor = Some(tf_id);
                }
            }

            if start_tf_id.is_none() {
                start_tf_id = Some(tf_id);
                predecessor_tf = Some(tf_id);
            }
            prev_tf = Some(tf_id);
            prev_output_field = output_field;
            if !appending {
                predecessor_tf = Some(tf_id);
                next_input_field = output_field;
            }
        }
        let mut term_state = TransformState::new(
            prev_output_field,
            prev_output_field,
            ms_id,
            default_batch_size,
            predecessor_tf,
            None,
            self.job_data.tf_mgr.claim_transform_ordering_id(),
        );
        let term_data = setup_tf_terminator(&mut self.job_data, &mut term_state);
        self.add_transform(term_state, term_data);
        Ok(start_tf_id.unwrap())
    }
    pub fn add_transform(&mut self, state: TransformState, data: TransformData<'a>) -> TransformId {
        let id = self.job_data.tf_mgr.transforms.claim_with_value(state);
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
                    TransformData::Format(tf) => handle_tf_format_stream_value_update(
                        &mut self.job_data,
                        svu.tf_id,
                        tf,
                        svu.sv_id,
                        svu.custom,
                    ),
                    TransformData::Regex(tf) => handle_tf_regex_stream_value_update(
                        &mut self.job_data,
                        svu.tf_id,
                        tf,
                        svu.sv_id,
                        svu.custom,
                    ),
                    TransformData::Split(_) => todo!(),
                    TransformData::Terminator(_) => unreachable!(),
                    TransformData::FileReader(_) => unreachable!(),
                    TransformData::Sequence(_) => unreachable!(),
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
                            self.handle_split_expansion(tf_id)?;
                        } else {
                            handle_tf_split(&mut self.job_data, tf_id, split);
                        }
                    }
                    TransformData::Print(tf) => handle_tf_print(jd, tf_id, tf),
                    TransformData::Regex(tf) => handle_tf_regex(jd, tf_id, tf),
                    TransformData::StringSink(tf) => handle_tf_string_sink(jd, tf_id, tf),
                    TransformData::FileReader(tf) => handle_tf_file_reader(jd, tf_id, tf),
                    TransformData::DataInserter(tf) => handle_tf_data_inserter(jd, tf_id, tf),
                    TransformData::Sequence(tf) => handle_tf_sequence(jd, tf_id, tf),
                    TransformData::Format(tf) => handle_tf_format(jd, tf_id, tf),
                    TransformData::Terminator(tf) => handle_tf_terminator(jd, tf_id, tf),
                    TransformData::Disabled => unreachable!(),
                }
                if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
                    if tf.mark_for_removal {
                        self.remove_transform(tf_id);
                    }
                }

                continue;
            }
            break;
        }
        Ok(())
    }
}
