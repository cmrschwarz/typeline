use std::{
    cell::{Cell, Ref, RefCell, RefMut},
    collections::{BinaryHeap, HashMap, VecDeque},
    sync::Arc,
};

use nonmax::{NonMaxU32, NonMaxUsize};
use smallvec::SmallVec;

use crate::{
    context::{ContextData, Job, Session, VentureDescription},
    field_data::{
        command_buffer::{ActionListIndex, ActionProducingFieldIndex, CommandBuffer},
        iter_hall::{IterHall, IterId},
        iters::{FieldIterator, Iter},
        record_buffer::RecordBuffer,
        FieldData,
    },
    operators::{
        call::{handle_eager_call_expansion, handle_lazy_call_expansion, setup_tf_call},
        call_concurrent::{
            handle_call_concurrent_expansion, handle_tf_call_concurrent,
            handle_tf_callee_concurrent, setup_callee_concurrent, setup_tf_call_concurrent,
        },
        cast::{handle_tf_cast, setup_tf_cast},
        count::{handle_tf_count, setup_tf_count},
        file_reader::{handle_tf_file_reader, setup_tf_file_reader},
        fork::{handle_fork_expansion, handle_tf_fork, setup_tf_fork},
        format::{handle_tf_format, handle_tf_format_stream_value_update, setup_tf_format},
        join::{handle_tf_join, handle_tf_join_stream_value_update, setup_tf_join},
        literal::{handle_tf_literal, setup_tf_literal},
        operator::{OperatorData, OperatorId},
        print::{handle_tf_print, handle_tf_print_stream_value_update, setup_tf_print},
        regex::{handle_tf_regex, handle_tf_regex_stream_value_update, setup_tf_regex},
        select::{handle_tf_select, setup_tf_select},
        sequence::{handle_tf_sequence, setup_tf_sequence},
        string_sink::{
            handle_tf_string_sink, handle_tf_string_sink_stream_value_update, setup_tf_string_sink,
        },
        terminator::{handle_tf_terminator, setup_tf_terminator},
        transform::{TransformData, TransformId, TransformOrderingId, TransformState},
    },
    ref_iter::AutoDerefIter,
    stream_value::{StreamValue, StreamValueData, StreamValueId},
    utils::universe::Universe,
    utils::{string_store::StringStoreEntry, temp_vec::TempVec},
};

pub const FIELD_REF_LOOKUP_ITER_ID: IterId = unsafe { IterId::new_unchecked(0) };
pub const ERROR_FIELD_PSEUDO_STR: usize = 0;

#[derive(Default)]
pub struct Field {
    pub field_id: FieldId, // used for checking whether we got rug pulled in case of cow
    pub ref_count: usize,
    //typically called on input fields, borrowing these mut is annoying
    pub clear_delay_request_count: Cell<usize>,
    pub has_unconsumed_input: Cell<bool>,

    pub match_set: MatchSetId,

    #[cfg(feature = "debug_logging")]
    pub producing_transform: Option<TransformId>,

    pub added_as_placeholder_by_tf: Option<TransformId>,

    pub min_apf_idx: Option<ActionProducingFieldIndex>,
    pub curr_apf_idx: Option<ActionProducingFieldIndex>,
    pub first_unapplied_al: ActionListIndex,

    pub names: SmallVec<[StringStoreEntry; 4]>,
    // fields potentially referenced by this field.
    // keeps them alive until this field is dropped
    pub field_refs: SmallVec<[FieldId; 4]>,
    pub cow_source: Option<FieldId>,
    pub field_data: IterHall,
}

pub type FieldId = NonMaxU32;
pub const INVALID_FIELD_ID: FieldId = unsafe { FieldId::new_unchecked(u32::MAX - 1) };

pub type MatchSetId = NonMaxUsize;

#[repr(C)]
pub struct MatchSet {
    pub stream_participants: Vec<TransformId>,
    pub command_buffer: CommandBuffer,
    pub field_name_map: HashMap<StringStoreEntry, FieldId>,
}

pub struct JobSession<'a> {
    pub transform_data: Vec<TransformData<'a>>,
    pub job_data: JobData<'a>,
    pub temp_vec: TempVec,
}
// a helper type so we can pass a transform handler typed
// TransformData + all the other Data of the WorkerThreadSession
pub struct JobData<'a> {
    pub session_data: &'a Session,
    pub tf_mgr: TransformManager,
    pub match_set_mgr: MatchSetManager,
    pub field_mgr: FieldManager,
    pub sv_mgr: StreamValueManager,
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

pub struct FieldManager {
    pub fields: Universe<FieldId, RefCell<Field>>,
}

pub struct MatchSetManager {
    pub match_sets: Universe<MatchSetId, MatchSet>,
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

impl Field {
    pub fn get_clear_delay_request_count(&self) -> usize {
        self.clear_delay_request_count.get()
    }
    pub fn has_unconsumed_input_or_equals(&self, value: bool) {
        self.has_unconsumed_input
            .set(self.has_unconsumed_input.get() || value);
    }
    pub fn request_clear_delay(&self) {
        self.has_unconsumed_input.set(true);
        self.clear_delay_request_count
            .set(self.clear_delay_request_count.get() + 1);
    }
    pub fn drop_clear_delay_request(&self) {
        self.clear_delay_request_count
            .set(self.clear_delay_request_count.get() - 1);
    }
}

impl TransformManager {
    pub fn claim_batch_with_limit(&mut self, tf_id: TransformId, limit: usize) -> (usize, bool) {
        let tf = &mut self.transforms[tf_id];
        let batch_size = tf.available_batch_size.min(limit);
        tf.available_batch_size -= batch_size;
        let input_done = tf.input_is_done && tf.available_batch_size == 0;
        (batch_size, input_done)
    }
    pub fn claim_batch(&mut self, tf_id: TransformId) -> (usize, bool) {
        self.claim_batch_with_limit(tf_id, self.transforms[tf_id].desired_batch_size)
    }
    pub fn claim_all(&mut self, tf_id: TransformId) -> (usize, bool) {
        self.claim_batch_with_limit(tf_id, usize::MAX)
    }
    pub fn unclaim_batch_size(&mut self, tf_id: TransformId, batch_size: usize) {
        self.transforms[tf_id].available_batch_size += batch_size;
    }
    pub fn claim_transform_ordering_id(&mut self) -> TransformOrderingId {
        let res = self.transform_ordering_id;
        self.transform_ordering_id = (self.transform_ordering_id.get() + 1).try_into().unwrap();
        res
    }
    pub fn inform_transform_batch_available(
        &mut self,
        tf_id: TransformId,
        batch_size: usize,
        any_prev_has_unconsumed_input: bool,
    ) {
        let tf = &mut self.transforms[tf_id];
        tf.available_batch_size += batch_size;
        tf.any_prev_has_unconsumed_input = any_prev_has_unconsumed_input;
        if tf.available_batch_size > 0 && !tf.is_ready {
            self.push_tf_in_ready_queue(tf_id);
        }
    }
    pub fn inform_successor_batch_available(&mut self, tf_id: TransformId, batch_size: usize) {
        let tf = &self.transforms[tf_id];
        if let Some(succ_tf_id) = tf.successor {
            self.inform_transform_batch_available(
                succ_tf_id,
                batch_size,
                tf.has_unconsumed_input(),
            );
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
        if tf.available_batch_size > 0 {
            self.push_tf_in_ready_queue(tf_id);
        }
    }
    pub fn maintain_single_value(
        &mut self,
        tf_id: TransformId,
        length: &mut Option<usize>,
        field_mgr: &FieldManager,
        match_set_mgr: &mut MatchSetManager,
        initial_call: bool,
        final_call_if_input_done: bool,
    ) -> (usize, bool) {
        let tf = &mut self.transforms[tf_id];
        let output_field_id = tf.output_field;
        let match_set_id = tf.match_set_id;
        let desired_batch_size = tf.desired_batch_size;
        let has_cont = tf.continuation.is_some();
        let max_batch_size = if let Some(len) = length {
            *len
        } else if has_cont {
            if !initial_call {
                if final_call_if_input_done {
                    field_mgr.fields[output_field_id]
                        .borrow_mut()
                        .field_data
                        .drop_last_value(1);
                }
                return (0, true);
            }
            1
        } else {
            usize::MAX
        };
        let (mut batch_size, mut input_done) =
            self.claim_batch_with_limit(tf_id, max_batch_size.min(desired_batch_size));
        if batch_size == 0 {
            if !initial_call {
                if final_call_if_input_done {
                    field_mgr.fields[output_field_id]
                        .borrow_mut()
                        .field_data
                        .drop_last_value(1);
                }
                return (0, true);
            }
            batch_size = length.unwrap_or(1);
        }
        if let Some(len) = length {
            *len -= batch_size;
            if *len == 0 {
                input_done = true;
            }
        } else if has_cont {
            input_done = true;
        }
        let mut output_field = field_mgr.fields[output_field_id].borrow_mut();
        match_set_mgr.match_sets[match_set_id]
            .command_buffer
            .execute_for_field(output_field_id, &mut output_field);
        // this results in always one more element being present than we advertise
        // as batch size. this prevents apply_field_actions from deleting
        // our value. unless we are done, in which case no additional value is inserted
        let drop_oversize = input_done && final_call_if_input_done;
        if batch_size == 0 && drop_oversize {
            output_field.field_data.drop_last_value(1);
        } else {
            output_field
                .field_data
                .dup_last_value(batch_size - drop_oversize as usize);
        }
        (batch_size, input_done)
    }
    pub fn prepare_for_output(
        &mut self,
        field_mgr: &FieldManager,
        match_set_mgr: &mut MatchSetManager,
        tf_id: TransformId,
        output_fields: impl IntoIterator<Item = FieldId>,
    ) {
        let tf = &mut self.transforms[tf_id];
        if tf.is_appending {
            tf.is_appending = false;
        } else {
            for ofid in output_fields {
                let mut f = field_mgr.fields[ofid].borrow_mut();
                if f.get_clear_delay_request_count() > 0 {
                    drop(f);
                    //TODO: this needs to preserve iterators
                    field_mgr.apply_field_actions(match_set_mgr, ofid);
                } else {
                    match_set_mgr.match_sets[tf.match_set_id]
                        .command_buffer
                        .drop_field_commands(ofid, &mut f);
                    f.field_data.clear();
                    f.has_unconsumed_input.set(false);
                }
            }
        }
    }
    pub fn prepare_output_field<'a>(
        &mut self,
        field_mgr: &'a FieldManager,
        match_set_mgr: &mut MatchSetManager,
        tf_id: TransformId,
    ) -> RefMut<'a, Field> {
        let output_field_id = self.transforms[tf_id].output_field;
        self.prepare_for_output(field_mgr, match_set_mgr, tf_id, [output_field_id]);
        field_mgr.fields[output_field_id].borrow_mut()
    }
}

impl MatchSetManager {
    pub fn add_field_name(&mut self, fm: &FieldManager, field_id: FieldId, name: StringStoreEntry) {
        let mut field = fm.fields[field_id].borrow_mut();
        if let Some(prev_field_id) = self.match_sets[field.match_set]
            .field_name_map
            .insert(name, field_id)
        {
            if prev_field_id != field_id {
                field.names.push(name);
                let mut prev_field = fm.fields[prev_field_id].borrow_mut();
                let pos = prev_field
                    .names
                    .iter()
                    .cloned()
                    .enumerate()
                    .filter_map(|(i, v)| if v == name { Some(i) } else { None })
                    .next()
                    .unwrap();
                prev_field.names.swap_remove(pos);
            }
        } else {
            field.names.push(name);
        }
    }
    pub fn add_match_set(&mut self) -> MatchSetId {
        self.match_sets.claim_with(|| MatchSet {
            stream_participants: Default::default(),
            command_buffer: Default::default(),
            field_name_map: Default::default(),
        })
    }
    pub fn remove_match_set(&mut self, _ms_id: MatchSetId) {
        todo!()
    }
}

impl FieldManager {
    pub fn get_min_apf_idx(&self, field_id: FieldId) -> Option<ActionProducingFieldIndex> {
        let field = self.fields[field_id].borrow();
        field.min_apf_idx
    }
    pub fn add_field(
        &mut self,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
    ) -> FieldId {
        self.add_field_with_data(ms_id, min_apf, FieldData::default())
    }
    pub fn add_field_with_data(
        &mut self,
        ms_id: MatchSetId,
        min_apf: Option<ActionProducingFieldIndex>,
        data: FieldData,
    ) -> FieldId {
        let id = self.fields.peek_claim_id();
        let mut field = Field {
            field_id: id,
            ref_count: 1,
            clear_delay_request_count: Cell::new(0),
            has_unconsumed_input: Cell::new(false),
            match_set: ms_id,
            added_as_placeholder_by_tf: None,
            min_apf_idx: min_apf,
            curr_apf_idx: None,
            first_unapplied_al: 0,
            names: Default::default(),
            cow_source: None,
            field_data: IterHall::new_with_data(data),
            #[cfg(feature = "debug_logging")]
            producing_transform: None,
            field_refs: Default::default(),
        };
        field.field_data.reserve_iter_id(FIELD_REF_LOOKUP_ITER_ID);
        self.fields.claim_with_value(RefCell::new(field));
        id
    }

    // this is usually called while iterating over an input field that contains field references
    // we therefore do NOT want to require a mutable reference over the field data, because that forces the caller to kill their iterator
    // instead we `split up` this struct to only require a mutable reference for the MatchSets, which we need to modify the command buffer
    pub fn apply_field_actions(&self, match_set_mgr: &mut MatchSetManager, field: FieldId) {
        let mut f = self.fields[field].borrow_mut();
        let match_set = f.match_set;
        let cb = &mut match_set_mgr.match_sets[match_set].command_buffer;
        if let Some(cow_source) = f.cow_source {
            if cb.requires_any_actions(&mut f) {
                let src = self.fields[cow_source].borrow();
                let mut iter = AutoDerefIter::new(&self, cow_source, src.field_data.iter());
                IterHall::copy_resolve_refs(match_set_mgr, &mut iter, &mut |func| {
                    func(&mut f.field_data)
                });
                let cb = &mut match_set_mgr.match_sets[match_set].command_buffer;
                cb.execute_for_field(field, &mut f);
                f.cow_source = None;
            }
        } else {
            cb.execute_for_field(field, &mut f);
        }
    }

    pub fn borrow_field_cow<'a>(
        &'a self,
        field_id: FieldId,
        mark_for_unconsumed_input: bool,
    ) -> Ref<'a, Field> {
        let field = self.fields[field_id].borrow();
        field.has_unconsumed_input_or_equals(mark_for_unconsumed_input);
        if let Some(cow_source) = field.cow_source {
            return self.fields[cow_source].borrow();
        }
        field
    }
    pub fn borrow_field_cow_mut<'a>(
        &'a self,
        field_id: FieldId,
        mark_for_unconsumed_input: bool,
    ) -> RefMut<'a, Field> {
        let field = self.fields[field_id].borrow_mut();
        field.has_unconsumed_input_or_equals(mark_for_unconsumed_input);
        if let Some(cow_source) = field.cow_source {
            return self.fields[cow_source].borrow_mut();
        }
        field
    }
    pub fn get_iter_cow_aware<'a>(
        &self,
        field_id: FieldId,
        field: &'a Field,
        iter_id: IterId,
    ) -> Iter<'a> {
        if field.field_id != field_id {
            let state = self.fields[field_id]
                .borrow()
                .field_data
                .get_iter_state(iter_id);
            return unsafe { field.field_data.get_iter_from_state(state) };
        }
        return field.field_data.get_iter(iter_id);
    }
    pub fn store_iter_cow_aware<'a>(
        &self,
        field_id: FieldId,
        field: &'a Field,
        iter_id: IterId,
        iter: impl FieldIterator<'a>,
    ) {
        if field.field_id != field_id {
            let iter_base = iter.into_base_iter();
            assert!(field.field_data.iter_is_from_iter_hall(&iter_base));
            let field = self.fields[field_id].borrow();
            unsafe { field.field_data.store_iter_unchecked(iter_id, iter_base) };
        } else {
            field.field_data.store_iter(iter_id, iter);
        }
    }

    pub fn bump_field_refcount(&self, field_id: FieldId) {
        self.fields[field_id].borrow_mut().ref_count += 1;
    }
    pub fn register_field_reference(&self, source: FieldId, target: FieldId) {
        self.fields[source].borrow_mut().field_refs.push(target);
        self.fields[target].borrow_mut().ref_count += 1;
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

impl<'a> JobData<'a> {
    pub fn new(sess: &'a Session) -> Self {
        Self {
            session_data: sess,
            tf_mgr: TransformManager {
                transforms: Default::default(),
                transform_ordering_id: Default::default(),
                ready_queue: Default::default(),
                stream_producers: Default::default(),
            },
            field_mgr: FieldManager {
                fields: Default::default(),
            },
            match_set_mgr: MatchSetManager {
                match_sets: Default::default(),
            },
            sv_mgr: Default::default(),
        }
    }
    pub fn unlink_transform(&mut self, tf_id: TransformId, available_batch_for_successor: usize) {
        let tf = &mut self.tf_mgr.transforms[tf_id];
        tf.mark_for_removal = true;
        let predecessor = tf.predecessor;
        let successor = tf.successor;
        let continuation = tf.continuation;
        let input_is_done = tf.input_is_done;
        let available_batch_size = tf.available_batch_size;
        let is_transparent = tf.is_transparent;
        if let Some(cont_id) = continuation {
            let cont = &mut self.tf_mgr.transforms[cont_id];
            cont.input_is_done = input_is_done;
            cont.successor = successor;
            cont.predecessor = predecessor;
            cont.available_batch_size = available_batch_size;
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
        if let Some(pred_id) = predecessor {
            self.tf_mgr.transforms[pred_id].successor = successor;
        }
        if let Some(succ_id) = successor {
            let succ = &mut self.tf_mgr.transforms[succ_id];
            succ.predecessor = predecessor;
            succ.input_is_done = true;
            let mut bs = available_batch_for_successor;
            if is_transparent {
                bs += available_batch_size;
            }
            // even if the current batch size is zero,
            // we want this guy ready because the end of input was reached
            succ.available_batch_size += bs;
            self.tf_mgr.push_tf_in_ready_queue(succ_id);
            //self.tf_mgr.inform_transform_batch_available(succ_id, bs);
        }
    }
    pub fn drop_field_refcount(&mut self, field_id: FieldId) {
        let mut field = self.field_mgr.fields[field_id].borrow_mut();
        field.ref_count -= 1;
        if field.ref_count == 0 {
            drop(field);
            self.remove_field(field_id);
        }
    }
    pub fn remove_field(&mut self, id: FieldId) {
        let mut field = self.field_mgr.fields[id].borrow_mut();
        #[cfg(feature = "debug_logging")]
        print!("removing field id {id} [ ");
        for n in &field.names {
            #[cfg(feature = "debug_logging")]
            print!("{} ", self.session_data.string_store.lookup(*n));
            self.match_set_mgr.match_sets[field.match_set]
                .field_name_map
                .remove(n);
        }
        #[cfg(feature = "debug_logging")]
        println!("]");
        let frs = std::mem::replace(&mut field.field_refs, Default::default());
        drop(field);
        self.field_mgr.fields.release(id);
        for fr in &frs {
            self.drop_field_refcount(*fr);
        }
    }
}

impl<'a> JobSession<'a> {
    pub fn log_state(&self, message: &str) {
        if cfg!(feature = "debug_logging") {
            println!("{message}");
            for (i, tf) in self.job_data.tf_mgr.transforms.iter_enumerated() {
                let name = if let Some(op_id) = tf.op_id {
                    self.job_data.session_data.operator_data[op_id as usize].default_op_name()
                } else {
                    "<unknown>".into()
                };
                println!("tf {} (ord id {}): {}", i, tf.ordering_id, name);
            }
            #[cfg(feature = "debug_logging")]
            for (i, f) in self.job_data.field_mgr.fields.iter_enumerated() {
                let field = f.borrow();
                print!(
                    "field {} (output of tf {:?}): [ ",
                    i, field.producing_transform
                );
                for n in &field.names {
                    let name = self.job_data.session_data.string_store.lookup(*n);
                    print!("{name} ")
                }
                println!("]");
            }
        }
    }
    pub fn setup_job(&mut self, mut job: Job) {
        self.job_data.match_set_mgr.match_sets.clear();
        self.job_data.field_mgr.fields.clear();
        self.job_data.tf_mgr.ready_queue.clear();
        let ms_id = self.job_data.match_set_mgr.add_match_set();
        //TODO: unpack record set properly here
        let input_record_count = job.data.adjust_field_lengths();
        let mut input_data = None;
        let mut input_data_fields = self.temp_vec.get();
        for fd in job.data.fields.into_iter() {
            let field_id = self
                .job_data
                .field_mgr
                .add_field_with_data(ms_id, None, fd.data);
            if let Some(name) = fd.name {
                self.job_data.match_set_mgr.add_field_name(
                    &self.job_data.field_mgr,
                    field_id,
                    name,
                );
            }
            input_data_fields.push(field_id);
            if input_data.is_none() {
                input_data = Some(field_id);
            }
        }
        let input_data = input_data.unwrap_or_else(|| {
            let field_id = self.job_data.field_mgr.add_field(ms_id, None);
            input_data_fields.push(field_id);
            field_id
        });

        let (start_tf_id, end_tf_id) =
            self.setup_transforms_from_op(ms_id, job.operator, input_data);
        self.add_terminator(end_tf_id);
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

        for input_field_id in input_data_fields.iter() {
            self.job_data.drop_field_refcount(*input_field_id);
        }
        self.temp_vec.store(input_data_fields);
        self.log_state("setting up job");
    }
    pub(crate) fn setup_venture(
        &mut self,
        _ctx: Option<&Arc<ContextData>>,
        buffer: Arc<RecordBuffer>,
        start_op_id: OperatorId,
    ) {
        self.job_data.match_set_mgr.match_sets.clear();
        self.job_data.field_mgr.fields.clear();
        self.job_data.tf_mgr.ready_queue.clear();
        let ms_id = self.job_data.match_set_mgr.add_match_set();

        let (start_tf_id, end_tf_id) = setup_callee_concurrent(self, ms_id, buffer, start_op_id);
        self.add_terminator(end_tf_id);
        self.job_data.tf_mgr.push_tf_in_ready_queue(start_tf_id);
        self.log_state("setting up venture");
    }

    pub fn remove_transform(&mut self, tf_id: TransformId) {
        let tf = &self.job_data.tf_mgr.transforms[tf_id];
        let tfif = tf.input_field;
        let tfof = tf.output_field;
        if tfif != INVALID_FIELD_ID {
            self.job_data.drop_field_refcount(tfif);
        }
        if tfof != INVALID_FIELD_ID {
            self.job_data.drop_field_refcount(tfof);
        }
        #[cfg(feature = "debug_logging")]
        {
            let tf = &self.job_data.tf_mgr.transforms[tf_id];
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

    pub fn setup_transforms_from_op(
        &mut self,
        ms_id: MatchSetId,
        start_op_id: OperatorId,
        chain_input_field_id: FieldId,
    ) -> (TransformId, TransformId) {
        let mut start_tf_id = None;
        let start_op = &self.job_data.session_data.operator_bases[start_op_id as usize];
        let default_batch_size = self.job_data.session_data.chains[start_op.chain_id as usize]
            .settings
            .default_batch_size;
        let mut prev_tf = None;
        let mut predecessor_tf = None;
        let mut next_input_field = chain_input_field_id;
        let mut prev_output_field = chain_input_field_id;
        let ops = &self.job_data.session_data.chains[start_op.chain_id as usize].operators
            [start_op.offset_in_chain as usize..];
        let mut mark_prev_field_as_placeholder = false;
        for op_id in ops {
            let op_base = &self.job_data.session_data.operator_bases[*op_id as usize];
            let op_data = &self.job_data.session_data.operator_data[*op_id as usize];
            let mut is_select = false;
            match op_data {
                OperatorData::Call(op) => {
                    if !op.lazy {
                        let (start_exp, end_exp) =
                            handle_eager_call_expansion(self, *op_id, ms_id, next_input_field);
                        return (start_tf_id.unwrap_or(start_exp), end_exp);
                    }
                }
                OperatorData::Key(op) => {
                    assert!(op_base.label.is_none()); //TODO
                    self.job_data.match_set_mgr.add_field_name(
                        &self.job_data.field_mgr,
                        prev_output_field,
                        op.key_interned,
                    );

                    continue;
                }
                OperatorData::Select(op) => {
                    if let Some(field_id) = self.job_data.match_set_mgr.match_sets[ms_id]
                        .field_name_map
                        .get(&op.key_interned)
                        .cloned()
                    {
                        next_input_field = field_id;
                    } else {
                        let field_id = self.job_data.field_mgr.add_field(
                            ms_id,
                            self.job_data.field_mgr.get_min_apf_idx(next_input_field),
                        );
                        self.job_data.match_set_mgr.add_field_name(
                            &self.job_data.field_mgr,
                            field_id,
                            op.key_interned,
                        );
                        next_input_field = field_id;
                    }
                    is_select = true;
                }
                _ => (),
            }
            let mut output_field = if is_select {
                self.job_data
                    .field_mgr
                    .bump_field_refcount(next_input_field);
                next_input_field
            } else if op_base.append_mode {
                self.job_data
                    .field_mgr
                    .bump_field_refcount(prev_output_field);
                prev_output_field
            } else {
                let min_apf = self.job_data.field_mgr.get_min_apf_idx(prev_output_field);
                self.job_data.field_mgr.add_field(ms_id, min_apf)
            };

            if let Some(name) = op_base.label {
                self.job_data.match_set_mgr.add_field_name(
                    &self.job_data.field_mgr,
                    output_field,
                    name,
                );
            }

            self.job_data
                .field_mgr
                .bump_field_refcount(next_input_field);
            let mut tf_state = TransformState::new(
                next_input_field,
                output_field,
                ms_id,
                default_batch_size,
                predecessor_tf,
                Some(*op_id),
                self.job_data.tf_mgr.claim_transform_ordering_id(),
            );
            tf_state.is_transparent = op_base.transparent_mode;
            tf_state.is_appending = op_base.append_mode;

            let tf_id_peek = self.job_data.tf_mgr.transforms.peek_claim_id();
            #[cfg(feature = "debug_logging")]
            {
                let mut of = self.job_data.field_mgr.fields[output_field].borrow_mut();
                if of.producing_transform.is_none() {
                    of.producing_transform = Some(tf_id_peek);
                }
            }
            if mark_prev_field_as_placeholder {
                let mut f = self.job_data.field_mgr.fields[next_input_field].borrow_mut();
                f.added_as_placeholder_by_tf = Some(tf_id_peek);
                mark_prev_field_as_placeholder = false;
            }
            let b = op_base;

            let jd = &mut self.job_data;
            let tf_data = match op_data {
                OperatorData::Cast(op) => setup_tf_cast(jd, b, op, &mut tf_state),
                OperatorData::Count(op) => setup_tf_count(jd, b, op, &mut tf_state),
                OperatorData::Fork(op) => setup_tf_fork(jd, b, op, &mut tf_state),
                OperatorData::Print(op) => setup_tf_print(jd, b, op, &mut tf_state),
                OperatorData::Join(op) => setup_tf_join(jd, b, op, &mut tf_state),
                OperatorData::Regex(op) => setup_tf_regex(jd, b, op, &mut tf_state),
                OperatorData::Format(op) => setup_tf_format(jd, b, op, tf_id_peek, &mut tf_state),
                OperatorData::StringSink(op) => setup_tf_string_sink(jd, b, op, &mut tf_state),
                OperatorData::FileReader(op) => setup_tf_file_reader(jd, b, op, &mut tf_state),
                OperatorData::Literal(op) => setup_tf_literal(jd, op_base, op, &mut tf_state),
                OperatorData::Sequence(op) => setup_tf_sequence(jd, op_base, op, &mut tf_state),
                OperatorData::Select(op) => setup_tf_select(jd, b, op, &mut tf_state),
                OperatorData::Call(op) => setup_tf_call(jd, b, op, &mut tf_state),
                OperatorData::CallConcurrent(op) => {
                    setup_tf_call_concurrent(jd, b, op, &mut tf_state)
                }
                OperatorData::Key(_) => unreachable!(),
                OperatorData::Next(_) => unreachable!(),
                OperatorData::Up(_) => unreachable!(),
            };
            output_field = tf_state.output_field;
            let appending = tf_state.is_appending;
            let transparent = tf_state.is_transparent;
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
                if !transparent {
                    next_input_field = output_field;
                }
            }
        }
        let start = start_tf_id.unwrap();
        let end = predecessor_tf.unwrap_or(start);
        (start, end)
    }
    pub fn add_terminator(&mut self, predecessor_tf_id: TransformId) {
        let ordering_id = self.job_data.tf_mgr.claim_transform_ordering_id();
        let tf_id = self.job_data.tf_mgr.transforms.peek_claim_id();
        let pred = &mut self.job_data.tf_mgr.transforms[predecessor_tf_id];
        let input_field = pred.output_field;
        let mut tf_state = TransformState::new(
            input_field,
            INVALID_FIELD_ID,
            pred.match_set_id,
            pred.desired_batch_size,
            Some(predecessor_tf_id),
            None,
            ordering_id,
        );
        pred.successor = Some(tf_id);
        let tf_data = setup_tf_terminator(&mut self.job_data, &mut tf_state);
        self.job_data.field_mgr.fields[input_field]
            .borrow_mut()
            .ref_count += 1;
        self.add_transform(tf_state, tf_data);
    }
    pub fn add_transform(&mut self, state: TransformState, data: TransformData<'a>) -> TransformId {
        let id = self.job_data.tf_mgr.transforms.claim_with_value(state);
        if self.transform_data.len() < self.job_data.tf_mgr.transforms.used_capacity() {
            self.transform_data
                .resize_with(self.job_data.tf_mgr.transforms.used_capacity(), || {
                    TransformData::Disabled
                });
        }
        self.transform_data[usize::from(id)] = data;
        id
    }
    fn handle_stream_value_update(&mut self, svu: StreamValueUpdate) {
        match &mut self.transform_data[usize::from(svu.tf_id)] {
            TransformData::Print(tf) => handle_tf_print_stream_value_update(
                &mut self.job_data,
                svu.tf_id,
                tf,
                svu.sv_id,
                svu.custom,
            ),
            TransformData::Join(tf) => handle_tf_join_stream_value_update(
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
            TransformData::Fork(_) => todo!(),
            TransformData::CallConcurrent(_) => todo!(),
            TransformData::Call(_) => unreachable!(),
            TransformData::Cast(_) => unreachable!(),
            TransformData::Count(_) => unreachable!(),
            TransformData::Select(_) => unreachable!(),
            TransformData::Terminator(_) => unreachable!(),
            TransformData::FileReader(_) => unreachable!(),
            TransformData::Sequence(_) => unreachable!(),
            TransformData::Disabled => unreachable!(),
            TransformData::Literal(_) => unreachable!(),
            TransformData::CalleeConcurrent(_) => unreachable!(),
        }
    }
    fn handle_transform(
        &mut self,
        tf_id: TransformId,
        ctx: Option<&Arc<ContextData>>,
    ) -> Result<(), VentureDescription> {
        match &mut self.transform_data[usize::from(tf_id)] {
            TransformData::Fork(fork) if !fork.expanded => handle_fork_expansion(self, tf_id, ctx)?,
            TransformData::Call(_) => handle_lazy_call_expansion(self, tf_id),
            TransformData::CallConcurrent(callcc) if !callcc.expanded => {
                handle_call_concurrent_expansion(self, tf_id, ctx)?
            }
            _ => (),
        }
        let jd = &mut self.job_data;
        match &mut self.transform_data[usize::from(tf_id)] {
            TransformData::Fork(fork) => handle_tf_fork(&mut self.job_data, tf_id, fork),
            TransformData::Print(tf) => handle_tf_print(jd, tf_id, tf),
            TransformData::Regex(tf) => handle_tf_regex(jd, tf_id, tf),
            TransformData::StringSink(tf) => handle_tf_string_sink(jd, tf_id, tf),
            TransformData::FileReader(tf) => handle_tf_file_reader(jd, tf_id, tf),
            TransformData::Literal(tf) => handle_tf_literal(jd, tf_id, tf),
            TransformData::Sequence(tf) => handle_tf_sequence(jd, tf_id, tf),
            TransformData::Format(tf) => handle_tf_format(jd, tf_id, tf),
            TransformData::Terminator(tf) => handle_tf_terminator(jd, tf_id, tf),
            TransformData::Join(tf) => handle_tf_join(jd, tf_id, tf),
            TransformData::Select(tf) => handle_tf_select(jd, tf_id, tf),
            TransformData::Count(tf) => handle_tf_count(jd, tf_id, tf),
            TransformData::Cast(tf) => handle_tf_cast(jd, tf_id, tf),
            TransformData::CallConcurrent(tf) => handle_tf_call_concurrent(jd, tf_id, tf),
            TransformData::CalleeConcurrent(tf) => handle_tf_callee_concurrent(jd, tf_id, tf),
            TransformData::Call(_) => (),
            TransformData::Disabled => unreachable!(),
        }
        if let Some(tf) = self.job_data.tf_mgr.transforms.get(tf_id) {
            if tf.mark_for_removal {
                self.remove_transform(tf_id);
            }
        }
        Ok(())
    }
    pub(crate) fn run(&mut self, ctx: Option<&Arc<ContextData>>) -> Result<(), VentureDescription> {
        loop {
            if let Some(svu) = self.job_data.sv_mgr.updates.pop_back() {
                self.handle_stream_value_update(svu);
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
                self.handle_transform(tf_id, ctx)?;
                continue;
            }
            return Ok(());
        }
    }
}
