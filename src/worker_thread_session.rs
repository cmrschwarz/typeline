use std::{
    cell::RefCell,
    collections::{hash_map, BinaryHeap, HashMap, VecDeque},
    iter,
};

use nonmax::NonMaxUsize;

use crate::{
    context::SessionData,
    field_data::{
        command_buffer::{ActionListIndex, ActionProducingFieldIndex, CommandBuffer},
        iter_hall::{IterHall, IterId},
        EntryId, FieldData,
    },
    operations::{
        data_inserter::{handle_tf_data_inserter, setup_tf_data_inserter},
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
    // number of tfs that might read this field
    // if this drops to zero, remove the field
    // TODO:
    // (unless this is named, we have a fwd, and this is not shadowed before it)
    pub ref_count: usize,
    pub match_set: MatchSetId,
    pub added_as_placeholder_by_tf: Option<TransformId>,
    pub min_apf_idx: Option<ActionProducingFieldIndex>,
    pub curr_apf_idx: Option<ActionProducingFieldIndex>,
    pub first_unapplied_al: ActionListIndex,

    pub name: Option<StringStoreEntry>,
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
    pub field_name_map: HashMap<StringStoreEntry, VecDeque<FieldId>>,
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
        field.name = name;
        field.match_set = ms_id;
        field.added_as_placeholder_by_tf = None;
        field.working_set_idx = None;
        field.min_apf_idx = min_apf;
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
    pub fn add_field_name(&mut self, field_id: FieldId, name: StringStoreEntry) {
        let mut field = self.fields[field_id].borrow_mut();
        if field.name.is_none() {
            field.name = Some(name);
        }
        match self.match_sets[field.match_set].field_name_map.entry(name) {
            hash_map::Entry::Occupied(mut e) => e.get_mut().push_back(field_id),
            hash_map::Entry::Vacant(e) => {
                e.insert(VecDeque::from_iter([field_id].into_iter()));
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
    pub fn claim_batch(&mut self, tf_id: TransformId) -> usize {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        let batch_size = tf.desired_batch_size.min(tf.available_batch_size);
        tf.available_batch_size -= batch_size;
        batch_size
    }
    pub fn unclaim_batch_size(&mut self, tf_id: TransformId, batch_size: usize) {
        self.tf_mgr.transforms[tf_id].available_batch_size += batch_size;
    }
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
    pub fn unlink_transform(&mut self, tf_id: TransformId, available_batch_for_successor: usize) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        tf.mark_for_removal = true;
        let predecessor = tf.predecessor;
        let successor = tf.successor;
        let continuation = tf.continuation;

        if let Some(cont_id) = continuation {
            let cont = &mut self.tf_mgr.transforms[cont_id];
            cont.successor = successor;
            cont.predecessor = predecessor;
            if let Some(pred_id) = predecessor {
                self.tf_mgr.transforms[pred_id].successor = continuation;
            }
            if let Some(succ_id) = successor {
                let succ = &mut self.tf_mgr.transforms[succ_id];
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
            self.tf_mgr.transforms[succ_id].predecessor = predecessor;
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
        for fd in job.data.fields.into_iter() {
            let field_id = self
                .job_data
                .record_mgr
                .add_field_with_data(ms_id, None, fd.name, fd.data);
            if input_data.is_none() {
                input_data = Some(field_id);
            }
            //TODO: add all to working set?
        }
        let input_data =
            input_data.unwrap_or_else(|| self.job_data.record_mgr.add_field(ms_id, None, None));

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
            let start_tf_id = self.setup_transforms_from_op(ms_id, job.starting_ops[0], input_data);

            let tf = &mut self.job_data.tf_mgr.transforms[start_tf_id];

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
        self.job_data.tf_mgr.transforms.release(tf_id);
        self.transform_data[usize::from(tf_id)] = TransformData::Disabled;
        //TODO: field refcounting
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
            let input_field = self.job_data.record_mgr.add_field(ms_id, None, None);
            targets[i] = self.setup_transforms_from_op(
                ms_id,
                <TransformId as TryInto<usize>>::try_into(op).unwrap() as OperatorId,
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
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
    ) -> TransformId {
        let mut start_tf_id = None;
        let start_op = &self.job_data.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.job_data.session_data.chains[start_op.chain_id as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = None;
        let mut predecessor_tf = None;
        let mut prev_field_id = chain_input_field_id;
        let mut output_field;
        let ops = &self.job_data.session_data.chains[start_op.chain_id as usize].operations
            [start_op.offset_in_chain as usize..];
        let mut mark_prev_field_as_placeholder = false;
        for op_id in ops {
            let op_base = &self.job_data.session_data.operator_bases[*op_id as usize];
            let op_data = &self.job_data.session_data.operator_data[*op_id as usize];
            let tf_data;
            let jd = &mut self.job_data;
            match op_data {
                OperatorData::Key(op) => {
                    jd.record_mgr.add_field_name(prev_field_id, op.key_interned);
                    if let Some(lbl) = op_base.label {
                        jd.record_mgr.add_field_name(prev_field_id, lbl);
                    }
                    continue;
                }
                OperatorData::Select(op) => {
                    if let Some(field_id) = jd.record_mgr.match_sets[ms_id]
                        .field_name_map
                        .get(&op.key_interned)
                        .and_then(|e| e.back())
                        .cloned()
                    {
                        prev_field_id = field_id;
                        if let Some(lbl) = op_base.label {
                            jd.record_mgr.add_field_name(field_id, lbl);
                        }
                    } else {
                        let field_id = jd.record_mgr.add_field(
                            ms_id,
                            jd.record_mgr.get_min_apf_idx(prev_field_id),
                            Some(op.key_interned),
                        );
                        prev_field_id = field_id;
                        //TODO: think about field refcounting
                        mark_prev_field_as_placeholder = true;
                    }
                    continue;
                }
                _ => (),
            }
            let mut tf_state = TransformState::new(
                prev_field_id,
                ms_id,
                default_batch_size,
                predecessor_tf,
                Some(*op_id),
                jd.tf_mgr.claim_transform_ordering_id(),
            );

            let tf_id_peek = jd.tf_mgr.transforms.peek_claim_id();
            if mark_prev_field_as_placeholder {
                let mut f = jd.record_mgr.fields[prev_field_id].borrow_mut();
                f.added_as_placeholder_by_tf = Some(tf_id_peek);
                mark_prev_field_as_placeholder = false;
            }
            (tf_data, output_field) = match &op_data {
                OperatorData::Split(split) => setup_tf_split(jd, split, &mut tf_state),
                OperatorData::Print(op) => setup_tf_print(jd, op, &mut tf_state),
                OperatorData::Regex(op) => setup_tf_regex(jd, op, &mut tf_state),
                OperatorData::Format(op) => setup_tf_format(jd, op, &mut tf_state, tf_id_peek),
                OperatorData::StringSink(op) => setup_tf_string_sink(jd, op, &mut tf_state),
                OperatorData::FileReader(op) => setup_tf_file_reader(jd, op, &mut tf_state),
                OperatorData::DataInserter(op) => setup_tf_data_inserter(jd, op, &mut tf_state),
                OperatorData::Sequence(op) => setup_tf_sequence(jd, op, &mut tf_state),
                OperatorData::Key(_) => unreachable!(),
                OperatorData::Select(_) => unreachable!(),
            };

            if let Some(lbl) = op_base.label {
                jd.record_mgr.add_field_name(output_field, lbl);
            }

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
            prev_field_id = output_field;
            if !appending {
                predecessor_tf = Some(tf_id);
            }
        }
        let mut term_state = TransformState::new(
            prev_field_id,
            ms_id,
            default_batch_size,
            predecessor_tf,
            None,
            self.job_data.tf_mgr.claim_transform_ordering_id(),
        );
        let term_data = setup_tf_terminator(&mut self.job_data, &mut term_state);
        self.add_transform(term_state, term_data);
        start_tf_id.unwrap()
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
                            self.handle_split_expansion(tf_id);
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
                    } else if tf.predecessor.is_none()
                        && tf.available_batch_size == 0
                        && !tf.is_ready
                        && tf.done_if_input_done
                    {
                        self.job_data.unlink_transform(tf_id, 0);
                    }
                }

                continue;
            }
            break;
        }
        Ok(())
    }
}
