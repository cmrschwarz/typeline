use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::{
    context::Session,
    document::TextEncoding,
    operators::operator::{OperatorData, OperatorId},
    selenium::SeleniumDownloadStrategy,
    utils::{
        get_two_distinct_mut,
        identity_hasher::BuildIdentityHasher,
        string_store::{
            StringStoreEntry, INVALID_STRING_STORE_ENTRY, INVALID_STRING_STORE_ENTRY_2,
        },
    },
};

pub type ChainId = u32;
pub const INVALID_CHAIN_ID: ChainId = ChainId::MAX;

#[derive(Clone, Copy)]
pub enum BufferingMode {
    BlockBuffer,
    LineBuffer,
    LineBufferStdin,
    LineBufferIfTTY,
    LineBufferStdinIfTTY,
}

#[derive(Clone)]
pub struct ChainSettings {
    pub default_text_encoding: TextEncoding,
    pub prefer_parent_text_encoding: bool,
    pub force_text_encoding: bool,
    pub selenium_download_strategy: SeleniumDownloadStrategy,
    pub default_batch_size: usize,
    pub stream_buffer_size: usize,
    pub stream_size_threshold: usize,
    pub buffering_mode: BufferingMode,
}

pub const DEFAULT_INPUT_FIELD: StringStoreEntry = INVALID_STRING_STORE_ENTRY;
const ANONYMOUS_INPUT_FIELD: StringStoreEntry = INVALID_STRING_STORE_ENTRY_2;

#[derive(Clone, Default)]
pub struct ChainLivenessData {
    // the boolean value specifies whether the fields are only read (-> false)
    // or potentially written (dup/drop) to (-> true)
    // the unnamed input field for the chain uses the special CHAIN_INPUT_FIELD value
    pub fields_accessed_before_assignment: HashMap<StringStoreEntry, bool, BuildIdentityHasher>,

    // new_name -> original name
    // only present if the field is not then shadowed (declared) by the chain
    pub field_name_aliases: HashMap<StringStoreEntry, StringStoreEntry>,

    pub fields_declared: HashSet<StringStoreEntry, BuildIdentityHasher>,
    pub successors: Vec<ChainId>,
    pub predecessors: Vec<ChainId>,

    pub contains_dynamic_accesses: bool,
    pub liveness_analysis_outdated: bool,
}

impl ChainLivenessData {
    fn unalias(&self, name: StringStoreEntry) -> StringStoreEntry {
        *self.field_name_aliases.get(&name).unwrap_or(&name)
    }
    fn add_field_name(&mut self, name_before: StringStoreEntry, new_name: StringStoreEntry) {
        self.field_name_aliases.insert(new_name, name_before);
        if self.fields_declared.contains(&name_before) {
            self.fields_declared.insert(new_name);
        }
    }
    fn add_field_name_unless_anon(
        &mut self,
        name_before: StringStoreEntry,
        new_name: StringStoreEntry,
    ) {
        if name_before != ANONYMOUS_INPUT_FIELD {
            self.add_field_name(name_before, new_name);
        }
    }
    fn declare_field(&mut self, name: StringStoreEntry) {
        self.fields_declared.insert(name);
        self.field_name_aliases.remove(&name);
    }
    fn mark_input_shadowed(&mut self, name: StringStoreEntry) {
        if name == DEFAULT_INPUT_FIELD {
            self.declare_field(name)
        }
    }
    fn add_successor(&mut self, chain_id: ChainId) {
        self.successors.push(chain_id)
    }
    fn access_field(&mut self, name: StringStoreEntry, write: bool) -> bool {
        let real_name = self.unalias(name);
        if !self.fields_declared.contains(&real_name) {
            self.set_field_accessed_before_assignment(real_name, write)
        } else {
            false
        }
    }
    fn access_field_unless_anon(&mut self, name: StringStoreEntry, write: bool) -> bool {
        if name != ANONYMOUS_INPUT_FIELD {
            self.access_field(name, write)
        } else {
            false
        }
    }
    fn set_field_accessed_before_assignment(
        &mut self,
        name: StringStoreEntry,
        write: bool,
    ) -> bool {
        match self.fields_accessed_before_assignment.entry(name) {
            Entry::Occupied(ref mut e) => {
                let prev = *e.get();
                e.insert(prev || write);
                return prev != write;
            }
            Entry::Vacant(e) => {
                e.insert(write);
                return true;
            }
        }
    }
}

#[derive(Clone)]
pub struct Chain {
    pub label: Option<StringStoreEntry>,
    pub settings: ChainSettings,
    pub operators: Vec<OperatorId>,
    pub subchains: Vec<ChainId>,
    pub liveness_data: ChainLivenessData,
}

pub fn compute_local_liveness_data(sess: &mut Session, chain_id: ChainId) {
    let cn = &mut sess.chains[chain_id as usize];
    let mut input_field = DEFAULT_INPUT_FIELD;
    let mut output_field = DEFAULT_INPUT_FIELD;
    let mut any_writes_so_far = false;

    for op_id in cn.operators.iter().cloned() {
        let op_base = &sess.operator_bases[op_id as usize];
        let transparent = op_base.transparent_mode;
        output_field = if op_base.append_mode {
            output_field
        } else {
            ANONYMOUS_INPUT_FIELD
        };
        let mut next_input_field = output_field;
        let mut input_accessed = true;
        let mut may_dup_or_drop = false;
        let mut input_referenced = false;
        match &sess.operator_data[op_id as usize] {
            OperatorData::Fork(_) => {
                for tgt in &cn.subchains {
                    cn.liveness_data.add_successor(*tgt);
                }
            }
            OperatorData::Jump(jump) => {
                cn.liveness_data.add_successor(jump.target_resolved);
            }
            OperatorData::Key(key) => {
                cn.liveness_data
                    .add_field_name_unless_anon(input_field, key.key_interned);
            }
            OperatorData::Select(select) => {
                next_input_field = cn.liveness_data.unalias(select.key_interned);
            }
            OperatorData::Regex(re) => {
                may_dup_or_drop = !re.opts.non_mandatory || re.opts.multimatch;
                for f in re.capture_group_names.iter().filter_map(|f| *f) {
                    cn.liveness_data.declare_field(f);
                }
                input_referenced = true;
            }
            OperatorData::Format(fmt) => {
                // might not technically be true, but we handle the access in here already
                input_accessed = false;
                for f in &fmt.refs_idx {
                    cn.liveness_data
                        .access_field_unless_anon(f.unwrap_or(input_field), any_writes_so_far);
                }
            }

            OperatorData::FileReader(_) => {
                // this only inserts if input is done, so no write flag neccessary
                input_accessed = false;
            }
            OperatorData::Literal(di) => {
                may_dup_or_drop = di.insert_count.is_some();
                input_accessed = false;
            }
            OperatorData::Join(_) => {
                may_dup_or_drop = true;
            }
            OperatorData::Sequence(seq) => {
                may_dup_or_drop = !seq.stop_after_input;
            }
            OperatorData::Count(_) => {
                may_dup_or_drop = true;
            }
            OperatorData::Cast(_) => (),
            OperatorData::Print(_) => (),
            OperatorData::StringSink(_) => (),
            OperatorData::Next(_) => unreachable!(),
            OperatorData::Up(_) => unreachable!(),
        }
        if input_accessed {
            cn.liveness_data.access_field_unless_anon(
                input_field,
                any_writes_so_far || (input_referenced && may_dup_or_drop),
            );
        }
        if let Some(label) = sess.operator_bases[op_id as usize].label {
            cn.liveness_data
                .add_field_name_unless_anon(output_field, label);
        }
        // because primitives like regex emits field references, we don't update the
        // current field in those cases and pretend people are still accessing
        // the original input field (which they are, through the FRs)
        if !transparent && !input_referenced {
            cn.liveness_data.mark_input_shadowed(input_field);
            input_field = next_input_field;
        }
        any_writes_so_far |= may_dup_or_drop;
    }
}
pub fn compute_field_livenses(sess: &mut Session) {
    // compute local liveness data (successors, accessed, declared)
    for c in 0..sess.chains.len() {
        compute_local_liveness_data(sess, c as ChainId);
    }
    let chains = &mut sess.chains;
    // compute predecessors by reversing successors
    for i in 0..chains.len() {
        let chain_id = i as ChainId;
        for succ_idx in 0..chains[i].liveness_data.successors.len() {
            let succ = chains[i].liveness_data.successors[succ_idx];
            chains[succ as usize]
                .liveness_data
                .predecessors
                .push(chain_id);
        }
    }
    // propagate liveness until it stabilizes
    let mut stack: Vec<ChainId> = Vec::new();
    for i in 0..chains.len() {
        chains[i].liveness_data.liveness_analysis_outdated = true;
    }
    stack.extend(0..chains.len() as ChainId);
    while let Some(chain_id) = stack.pop() {
        let chain_id = chain_id as usize;
        chains[chain_id].liveness_data.liveness_analysis_outdated = false;
        let succ_count = chains[chain_id].liveness_data.successors.len();
        let mut predecessors_need_update = false;
        for succ_n in 0..succ_count {
            let succ_id = chains[chain_id].liveness_data.successors[succ_n] as usize;
            let (cn, succ) = get_two_distinct_mut(chains, chain_id, succ_id);
            for (f, write) in &succ.liveness_data.fields_accessed_before_assignment {
                if !cn.liveness_data.fields_declared.contains(f) {
                    predecessors_need_update |= cn
                        .liveness_data
                        .set_field_accessed_before_assignment(*f, *write);
                }
            }
            if succ.liveness_data.contains_dynamic_accesses {
                if !cn.liveness_data.contains_dynamic_accesses {
                    cn.liveness_data.contains_dynamic_accesses = true;
                    predecessors_need_update = true;
                }
            }
        }
        if predecessors_need_update {
            let pred_count = chains[chain_id].liveness_data.predecessors.len();
            for pred_n in 0..pred_count {
                let pred_id = chains[chain_id].liveness_data.predecessors[pred_n] as usize;
                let pred = &mut chains[pred_id];
                if pred.liveness_data.liveness_analysis_outdated == false {
                    pred.liveness_data.liveness_analysis_outdated = true;
                    stack.push(pred_id as ChainId);
                }
            }
        }
    }
}
