mod helpers;

use handlebars::{Handlebars, RenderError, RenderErrorReason};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::{cell::Cell, collections::HashSet, fmt::Write, io::BufWriter};

use crate::{
    job::JobData,
    operators::{
        aggregator::TfAggregatorHeader,
        fork::TfFork,
        forkcat::{FcSubchainIdx, TfForkCat},
        transform::{TransformData, TransformId},
    },
    options::chain_settings::RationalsPrintMode,
    record_data::{
        field::{Field, FieldId},
        field_action::FieldAction,
        field_value::FieldValue,
        field_value_ref::FieldValueRef,
        formattable::{
            FormatOptions, Formattable, FormattingContext, RealizedFormatKey,
            TypeReprFormat,
        },
        group_track::{GroupTrack, GroupTrackId, GroupTrackIterState},
        iter_hall::{CowVariant, IterKind, IterState},
        iters::{FieldDataRef, FieldIter, FieldIterator},
        match_set::MatchSetId,
        scope_manager::ScopeValue,
        stream_value::{
            StreamValueData, StreamValueDataOffset, StreamValueUpdate,
        },
    },
    utils::{
        index_slice::IndexSlice, indexing_type::IndexingType,
        lazy_lock_guard::LazyRwLockGuard, maybe_text::MaybeText,
        string_store::StringStore,
    },
};

struct MatchChain {
    ms_id: MatchSetId,
    start_tf_id: Option<TransformId>,
    tf_envs: Vec<TransformEnv>,
    dead_slots: Vec<usize>,
}

struct TransformEnv {
    tf_id: Option<TransformId>,
    group_tracks: Vec<GroupTrackId>,
    subchains: Vec<TransformChain>,
    fields: Vec<FieldId>,
}

struct TransformChain {
    match_chains: Vec<MatchChain>,
}

#[derive(Clone, Debug)]
pub struct FieldInfo {
    pub id: Option<FieldId>,
    pub name: String,
    pub producing_arg: Option<String>,
    pub cow_info: Option<CowInfo>,
}

#[derive(Clone, Copy, Debug)]
pub struct CowInfo {
    pub source: Option<FieldId>,
    pub variant: CowVariant,
}

impl MatchChain {
    pub fn new(ms_id: MatchSetId, start_tf: Option<TransformId>) -> Self {
        Self {
            ms_id,
            start_tf_id: start_tf,
            tf_envs: Vec::new(),
            dead_slots: Vec::new(),
        }
    }
    pub fn from_start_tf(jd: &JobData, start_tf: TransformId) -> Self {
        Self::new(jd.tf_mgr.transforms[start_tf].match_set_id, Some(start_tf))
    }
}

static TEMPLATES: Lazy<Handlebars> = Lazy::new(|| {
    let mut hb = Handlebars::new();
    hb.register_template_string("head", include_str!("head.hbs"))
        .unwrap();
    hb.register_template_string("tail", include_str!("tail.hbs"))
        .unwrap();
    hb.register_template_string("update", include_str!("update.hbs"))
        .unwrap();
    hb.register_partial("stylesheet", include_str!("style.css"))
        .unwrap();
    hb.register_partial("script", include_str!("script.js"))
        .unwrap();
    hb.register_partial("iter_list", include_str!("iter_list.hbs"))
        .unwrap();
    hb.register_partial("field", include_str!("field.hbs"))
        .unwrap();
    hb.register_partial("group_track", include_str!("group_track.hbs"))
        .unwrap();

    hb.register_partial(
        "transform_chain",
        include_str!("transform_chain.hbs"),
    )
    .unwrap();
    hb.register_partial("transform_env", include_str!("transform_env.hbs"))
        .unwrap();

    hb.register_partial("stream_values", include_str!("stream_values.hbs"))
        .unwrap();

    hb.register_helper("unique_id", Box::new(helpers::UniqueId));
    hb.register_helper("repeat", Box::new(helpers::helper_repeat));
    hb.register_helper("let", Box::new(helpers::helper_let));
    hb.register_helper("range", Box::new(helpers::Range));
    hb.register_helper("reindent", Box::new(helpers::Reindent));
    hb.register_helper("debug_log", Box::new(helpers::DebugLog));
    hb.register_helper("stringify", Box::new(helpers::Stringify));
    hb.register_helper("to_int", Box::new(helpers::ToInt));
    hb.register_helper("add", Box::new(helpers::Add));
    hb.set_strict_mode(true);
    hb
});

#[allow(clippy::needless_pass_by_value)]
fn unwrap_render_error(re: RenderError) -> std::io::Error {
    match RenderErrorReason::from(re) {
        RenderErrorReason::IOError(e) => e,
        reason => panic!("handlebars template error in debug log: {reason}"),
    }
}

fn add_field_data_dead_slots<'a>(
    fd: impl FieldDataRef<'a>,
    dead_slots: &mut [usize],
) {
    let mut iter = FieldIter::from_start_allow_dead(fd);
    for ds in dead_slots {
        *ds = (*ds).max(iter.skip_dead_fields());
        #[cfg(feature = "debug_log_lenient")]
        if !iter.is_next_valid() {
            break;
        }
        iter.next_field_allow_dead();
    }
}

fn add_group_track_dead_slots(gt: &GroupTrack, dead_slots: &mut Vec<usize>) {
    let passed_field_count = gt.passed_fields_count;

    let mut iter = gt.iter();

    loop {
        let pos = iter.field_pos();

        let mut empty_groups = iter.skip_empty_groups();
        if pos == passed_field_count {
            empty_groups += passed_field_count;
        }
        if pos >= dead_slots.len() {
            dead_slots.push(0);
        }
        let ds = &mut dead_slots[pos];
        *ds = (*ds).max(empty_groups);

        #[cfg(feature = "debug_log_lenient")]
        if !iter.is_last_group() && iter.is_end_of_group(true) {
            break;
        }
        iter.next_n_fields(1);
        if iter.is_end_of_group(true) && !iter.try_next_group() {
            break;
        }
    }
}

fn setup_transform_chain_dead_slots(tc: &mut TransformChain, jd: &JobData) {
    for mc in &mut tc.match_chains {
        for env in &mc.tf_envs {
            for &field_id in &env.fields {
                if !cfg!(feature = "debug_log_no_apply") {
                    // we have to do this here because doing json in a second
                    // phase means that the fields have not been touched yet
                    jd.field_mgr.apply_field_actions(
                        &jd.match_set_mgr,
                        field_id,
                        true,
                    );
                }
                let cfr = jd.field_mgr.get_cow_field_ref_raw(field_id);
                let fc = cfr.destructured_field_ref().field_count();

                mc.dead_slots.resize(mc.dead_slots.len().max(fc), 0);
                // if we don't apply, dead fields won't line up anyways
                // so we don't use dead slots
                if !cfg!(feature = "debug_log_no_apply") {
                    add_field_data_dead_slots(&cfr, &mut mc.dead_slots[0..fc]);
                }
            }
            for &gt_id in &env.group_tracks {
                if !cfg!(feature = "debug_log_no_apply") {
                    // we have to do this here because doing json in a second
                    // phase means that the fields have not been touched yet
                    jd.group_track_manager
                        .apply_actions_to_track(&jd.match_set_mgr, gt_id);
                }
                let gt = jd.group_track_manager.group_tracks[gt_id].borrow();
                let fc = gt.group_lengths.iter().sum::<usize>()
                    + gt.passed_fields_count;
                mc.dead_slots.resize(mc.dead_slots.len().max(fc), 0);
                add_group_track_dead_slots(&gt, &mut mc.dead_slots)
            }
        }
    }
}

fn setup_transform_chain_tf_envs(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_match_chain: MatchChain,
) -> TransformChain {
    let mut match_chains = Vec::new();
    let mut match_chain = start_match_chain;
    if let Some(mut tf_id) = match_chain.start_tf_id {
        loop {
            let succ = setup_transform_tf_envs(
                jd,
                tf_data,
                tf_id,
                &mut match_chains,
                &mut match_chain,
            );

            if let Some(succ) = succ {
                tf_id = succ;
            } else {
                break;
            }
        }
    }
    match_chains.push(match_chain);
    for mc in &mut match_chains {
        add_hidden_fields(jd, mc);
    }
    TransformChain { match_chains }
}

fn setup_transform_tf_envs(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_id: TransformId,
    match_chains: &mut Vec<MatchChain>,
    match_chain: &mut MatchChain,
) -> Option<TransformId> {
    let tf = &jd.tf_mgr.transforms[tf_id];
    let mut succ = tf.successor;
    match &tf_data[tf_id] {
        TransformData::ForkCat(fc) => {
            let mut fc_mc = setup_forkcat_match_chain(jd, tf_data, fc, tf_id);
            std::mem::swap(match_chain, &mut fc_mc);
            match_chains.push(fc_mc);
        }
        TransformData::Fork(fc) => {
            match_chain
                .tf_envs
                .push(setup_fork_tf_env(jd, tf_data, fc, tf_id));
        }
        TransformData::AggregatorHeader(agg) => {
            succ = Some(setup_aggregator_tf_envs(
                jd,
                tf_data,
                agg,
                tf_id,
                match_chains,
                match_chain,
            ));
        }

        _ => {
            let mut fields = Vec::new();
            let mut group_tracks = Vec::new();
            let tf = &jd.tf_mgr.transforms[tf_id];
            if tf.output_group_track_id != tf.input_group_track_id {
                group_tracks.push(tf.output_group_track_id);
            }
            tf_data[tf_id].get_out_fields(tf, &mut fields);
            match_chain.tf_envs.push(TransformEnv {
                tf_id: Some(tf_id),
                group_tracks,
                subchains: Vec::new(),
                fields,
            });
        }
    }
    succ
}

fn setup_aggregator_tf_envs(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    agg: &TfAggregatorHeader,
    tf_id: TransformId,
    match_chains: &mut Vec<MatchChain>,
    match_chain: &mut MatchChain,
) -> TransformId {
    match_chain.tf_envs.push(TransformEnv {
        tf_id: Some(tf_id),
        group_tracks: Vec::new(),
        subchains: Vec::new(),
        fields: Vec::new(),
    });

    for &sub_tf_id in &agg.sub_tfs {
        setup_transform_tf_envs(
            jd,
            tf_data,
            sub_tf_id,
            match_chains,
            match_chain,
        );
    }

    agg.trailer_tf_id
}

fn setup_fork_tf_env(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    fork: &TfFork,
    tf_id: TransformId,
) -> TransformEnv {
    let mut subchains = Vec::new();

    for tgt in &fork.targets {
        let mut sc_match_chain = MatchChain::from_start_tf(jd, tgt.tf_id);
        let mut input_tf_env = TransformEnv {
            tf_id: None,
            group_tracks: vec![tgt.gt_id],
            subchains: Vec::new(),
            fields: Vec::new(),
        };
        push_field_component_with_refs(
            jd,
            jd.tf_mgr.transforms[tgt.tf_id].input_field,
            &mut input_tf_env.fields,
        );
        sc_match_chain.tf_envs.push(input_tf_env);
        let mut subchain =
            setup_transform_chain_tf_envs(jd, tf_data, sc_match_chain);
        setup_transform_chain_dead_slots(&mut subchain, jd);
        subchains.push(subchain);
    }

    TransformEnv {
        tf_id: Some(tf_id),
        group_tracks: Vec::new(),
        subchains,
        fields: Vec::new(),
    }
}

fn setup_forkcat_match_chain(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    fc: &TfForkCat,
    tf_id: TransformId,
) -> MatchChain {
    let fc_cont = fc.continuation_state.lock().unwrap();

    let mut match_chain = MatchChain::new(
        fc_cont.continuation_ms_id,
        fc_cont.continuation_tf_id,
    );
    let mut subchains = Vec::new();
    let mut fields = Vec::new();
    let mut group_tracks = Vec::new();

    for sce in &fc_cont.subchains {
        let mut sc_match_chain =
            MatchChain::from_start_tf(jd, sce.start_tf_id);
        let mut input_tf_env = TransformEnv {
            tf_id: None,
            group_tracks: vec![
                jd.tf_mgr.transforms[sce.start_tf_id].input_group_track_id,
            ],
            subchains: Vec::new(),
            fields: Vec::new(),
        };
        push_field_component_with_refs(
            jd,
            jd.tf_mgr.transforms[sce.start_tf_id].input_field,
            &mut input_tf_env.fields,
        );
        sc_match_chain.tf_envs.push(input_tf_env);
        let mut subchain =
            setup_transform_chain_tf_envs(jd, tf_data, sc_match_chain);
        setup_transform_chain_dead_slots(&mut subchain, jd);
        subchains.push(subchain);
    }
    if let Some(TransformData::ForkCatSubchainTrailer(trailer)) = fc_cont
        .subchains
        .get(FcSubchainIdx::zero())
        .map(|sc| &tf_data[sc.trailer_tf_id])
    {
        for cont_mapping in fc_cont.subchains[trailer.subchain_idx]
            .continuation_field_mappings
            .iter()
            .rev()
        {
            push_field_component_with_refs(
                jd,
                cont_mapping.cont_field_id,
                &mut fields,
            );
        }
    };

    group_tracks.push(fc_cont.continuation_input_group_track);

    match_chain.tf_envs.push(TransformEnv {
        tf_id: Some(tf_id),
        group_tracks,
        subchains,
        fields,
    });

    match_chain
}

fn push_field_component_with_refs(
    jd: &JobData,
    field: FieldId,
    fields: &mut Vec<FieldId>,
) {
    fields.extend_from_slice(&jd.field_mgr.fields[field].borrow().field_refs);
    fields.push(field);
}

fn match_chain_to_json(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    match_chain: &MatchChain,
) -> serde_json::Value {
    let mut envs = Vec::new();
    let string_store = jd.session_data.string_store.read().unwrap();
    for (tf_env_n, tf_env) in match_chain.tf_envs.iter().enumerate() {
        let mut subchains = Vec::new();
        for tf_chain in &tf_env.subchains {
            subchains.push(transform_chain_to_json(jd, tf_data, tf_chain));
        }
        let mut fields = Vec::new();
        for &field_id in &tf_env.fields {
            fields.push(field_to_json(
                jd,
                &jd.field_mgr.fields[field_id].borrow(),
                field_id,
                &string_store,
                &match_chain.dead_slots,
            ))
        }
        let mut group_tracks = Vec::new();
        for &gt in &tf_env.group_tracks {
            group_tracks.push(group_track_to_json(
                jd,
                gt,
                &match_chain.dead_slots,
            ));
        }

        let unique_id = tf_env
            .tf_id
            .map(|i| Value::Number(i.into_usize().into()))
            .unwrap_or(Value::String(format!(
                "{}-{}",
                match_chain.ms_id, tf_env_n
            )));

        envs.push(json!({
            "transform_id": tf_env.tf_id.map(IndexingType::into_usize),
            "transform_id_unique": unique_id,
            "transform_display_name": tf_env.tf_id.map(|id|tf_data[id].display_name().to_string()),
            "subchains": subchains,
            "fields": fields,
            "group_tracks": group_tracks
        }));
    }
    json!({
        "ms_id": match_chain.ms_id.into_usize(),
        "tf_envs": envs,
    })
}

fn transform_chain_to_json(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_chain: &TransformChain,
) -> Value {
    json!({
        "match_chains": tf_chain.match_chains.iter().map(|mc|{
            match_chain_to_json(jd, tf_data, mc)
        }).collect::<Vec<_>>()
    })
}

fn add_hidden_fields(jd: &JobData, mc: &mut MatchChain) {
    let mut shown_fields = HashSet::new();
    for tfe in &mc.tf_envs {
        for field in &tfe.fields {
            shown_fields.insert(*field);
        }
    }
    let mut hidden_fields = Vec::new();
    for (field_id, field) in jd.field_mgr.fields.iter_enumerated() {
        if field.borrow().match_set != mc.ms_id {
            continue;
        }
        if shown_fields.contains(&field_id) {
            continue;
        }
        hidden_fields.push(field_id);
    }
    hidden_fields.extend_from_slice(&mc.tf_envs[0].fields);
    mc.tf_envs[0].fields = hidden_fields;
}

fn setup_transform_chain(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
) -> TransformChain {
    let mut start_match_chain = MatchChain::from_start_tf(jd, start_tf);
    let start_tf = &jd.tf_mgr.transforms[start_tf];
    start_match_chain.tf_envs.push(TransformEnv {
        tf_id: None,
        subchains: Vec::new(),
        fields: vec![start_tf.input_field],
        group_tracks: vec![start_tf.input_group_track_id],
    });

    let mut tf_chain =
        setup_transform_chain_tf_envs(jd, tf_data, start_match_chain);

    setup_transform_chain_dead_slots(&mut tf_chain, jd);
    tf_chain
}

pub fn iter_kind_to_json(
    kind: &IterKind,
    tooltip_text: &str,
) -> Option<Value> {
    Some(match kind {
        IterKind::Undefined => json!({
            "transform_id": Value::Null,
            "cow_field_id": Value::Null,
            "display_text": "undef",
            "tooltip_text": tooltip_text
        }),
        IterKind::Transform(tf_id) => json!({
            "transform_id": tf_id.into_usize(),
            "cow_field_id": Value::Null,
            "display_text": format!("tf {tf_id}"),
            "tooltip_text": tooltip_text
        }),
        IterKind::CowField(cow_field_id) => json!({
            "transform_id": Value::Null,
            "cow_field_id": cow_field_id.into_usize(),
            "display_text": format!("cow {cow_field_id}"),
            "tooltip_text": tooltip_text
        }),
        IterKind::RefLookup => return None,
    })
}

pub fn iters_to_json(iters: &[IterState]) -> Value {
    return Value::Array({
        iters
            .iter()
            .filter_map(|i| {
                iter_kind_to_json(
                    &i.kind,
                    &format!(
                        "HID: {}, HOFF: {}, FP: {}, D: {}, FRLA: {}",
                        i.header_idx,
                        i.header_rl_offset,
                        i.field_pos,
                        i.data,
                        i.first_right_leaning_actor_id.into_usize()
                    ),
                )
            })
            .collect::<Vec<_>>()
    });
}

pub fn field_data_to_json<'a>(
    jd: &JobData,
    fd: impl FieldDataRef<'a>,
    field_count_cap: usize,
    field_info: &FieldInfo,
    field_refs: &[FieldId],
    dead_slots: &[usize],
    pending_actions: &[FieldAction],
    mut iters: Vec<IterState>,
) -> serde_json::Value {
    iters.sort_by(|is1, is2| match is1.header_idx.cmp(&is2.header_idx) {
        std::cmp::Ordering::Equal => {
            is1.header_rl_offset.cmp(&is2.header_rl_offset)
        }
        res @ (std::cmp::Ordering::Less | std::cmp::Ordering::Greater) => res,
    });
    let mut iter = FieldIter::from_start_allow_dead(&fd);

    let mut del_count = 0;
    let mut string_store = LazyRwLockGuard::new(&jd.session_data.string_store);
    let mut formatting_context = FormattingContext {
        ss: Some(&mut string_store),
        fm: Some(&jd.field_mgr),
        msm: Some(&jd.match_set_mgr),
        rationals_print_mode: RationalsPrintMode::Dynamic,
        is_stream_value: false,
        rfk: RealizedFormatKey {
            opts: FormatOptions {
                type_repr: TypeReprFormat::Typed,
                ..FormatOptions::default()
            },
            ..RealizedFormatKey::default()
        },
    };

    let mut rows = Vec::new();

    let iters_before_start = iters
        .iter()
        .position(|i| i.field_pos > 0)
        .unwrap_or(iters.len());
    let mut iters_start = iters_before_start;

    while iter.is_next_valid() && iter.get_next_field_pos() < field_count_cap {
        let is_end_of_curr_header = iter.field_run_length_fwd() == 1;
        let header_idx = iter.get_next_header_index();
        let header_offs = iter.field_run_length_bwd();
        let mut iters_end = iters_start;

        while iters_end < iters.len() {
            let it = &iters[iters_end];

            if it.header_rl_offset > header_offs + 1 {
                break;
            }
            if it.header_idx > header_idx
                && (!is_end_of_curr_header || it.header_rl_offset > 0)
            {
                break;
            }
            if it.header_idx > header_idx + 1 {
                break;
            }
            iters_end += 1;
        }
        let h = fd.headers()[iter.get_next_header_index()];
        let dead_slot_count = if cfg!(feature = "debug_log_no_apply") {
            0
        } else if h.deleted() {
            del_count += 1;
            0
        } else {
            let del_count_prev = del_count;
            del_count = 0;
            dead_slots[iter.get_next_field_pos()] - del_count_prev
        };
        let shadow_meta = iter.field_run_length_bwd() != 0;
        let shadow_data = shadow_meta && h.shared_value();

        let value = iter.get_next_typed_field().value;
        let mut value_str = MaybeText::default();
        match value {
            FieldValueRef::FieldReference(fr) => {
                value_str = MaybeText::Text(format!("{}", fr.field_ref_offset))
            }
            FieldValueRef::SlicedFieldReference(fr) => {
                value_str = MaybeText::Text(format!(
                    "({})[{}..{}]",
                    fr.field_ref_offset, fr.begin, fr.end
                ))
            }
            FieldValueRef::StreamValueId(sv_id) => {
                value_str = MaybeText::Text(sv_id.to_string())
            }
            _ => {
                Formattable::format(
                    &value,
                    &mut formatting_context,
                    &mut value_str,
                )
                .unwrap();
            }
        }

        iter.next_field_allow_dead();
        if cfg!(feature = "debug_log_lenient") && !iter.is_next_valid()
            || iter.get_next_field_pos() >= field_count_cap
        {
            iters_end = iters.len();
        }

        rows.push(json!({
            "dead_slots": dead_slot_count,
            "shadow_meta": shadow_meta,
            "shadow_data": shadow_data,
            "repr": h.fmt.repr.to_string(),
            "padding": if header_offs == 0 { h.fmt.leading_padding() } else {0},
            "deleted": h.deleted(),
            "shared": h.shared_value(),
            "size": h.size,
            "run_length": h.run_length,
            "same_as_prev": h.same_value_as_previous(),
            "value": value_str.into_text_lossy(),
            "iters": iters_to_json(&iters[iters_start..iters_end])

        }));
        iters_start = iters_end;
    }

    assert_eq!(iters_start, iters.len());

    let cow = if let Some(info) = field_info.cow_info {
        let variant = match info.variant {
            CowVariant::FullCow => "full-cow",
            CowVariant::DataCow => "data-cow",
            CowVariant::SameMsCow => "same-ms-cow",
            CowVariant::RecordBufferDataCow => "rb-data-cow",
            CowVariant::RecordBufferFullCow => "rb-full-cow",
        };
        json!({
            "variant": variant,
            "source": info.source.map(IndexingType::into_usize),
        })
    } else {
        Value::Null
    };

    let mut res = json!({
        "id": field_info.id.map(IndexingType::into_usize),
        "name": field_info.name,
        "producing_arg": field_info.producing_arg,
        "cow": cow,
        "field_refs": field_refs.iter().map(|v|v.into_usize()).collect::<Vec<_>>(),
        "rows": rows,
        "iters":  iters_to_json(&iters),
        "iters_before_start": iters_to_json(&iters[..iters_before_start]),
    });

    if cfg!(feature = "debug_log_no_apply") {
        let mut pending_actions_json = Vec::new();
        for action in pending_actions {
            pending_actions_json.push(json!({
                "field_pos": action.field_idx,
                "kind": action.kind.to_str(),
                "run_length": action.run_len,
            }));
        }
        res.as_object_mut().unwrap().insert(
            "pending_actions".to_string(),
            pending_actions_json.into(),
        );
    }

    res
}

pub fn field_to_json(
    jd: &JobData,
    field: &Field,
    field_id: FieldId,
    string_store: &StringStore,
    dead_slots: &[usize],
) -> serde_json::Value {
    let cow_info = if let Some(variant) = field.iter_hall.cow_variant() {
        let (cow_src_field, _) =
            field.iter_hall.cow_source_field(&jd.field_mgr);
        Some(CowInfo {
            source: cow_src_field,
            variant,
        })
    } else {
        None
    };

    let field_count_cow =
        if field.iter_hall.cow_variant() == Some(CowVariant::FullCow) {
            field
                .iter_hall
                .get_cow_iter_state(&jd.field_mgr)
                .unwrap()
                .field_pos
        } else {
            usize::MAX
        };

    #[allow(unused_mut)]
    let mut producing_arg = None;
    #[cfg(feature = "debug_logging")]
    if !field.producing_transform_arg.is_empty() {
        producing_arg = Some(field.producing_transform_arg.to_string());
    }

    let mut field_name = String::new();

    for (i, (&name, value)) in jd.scope_mgr.scopes
        [jd.match_set_mgr.match_sets[field.match_set].active_scope]
        .values
        .iter()
        .enumerate()
    {
        let &ScopeValue::Field(sym_field_id) = value else {
            continue;
        };
        if sym_field_id != field_id {
            continue;
        }
        if i > 0 {
            field_name.push_str(" / ");
        }
        field_name
            .write_fmt(format_args!("@{}", string_store.lookup(name)))
            .unwrap();
    }

    let field_info = FieldInfo {
        id: Some(field_id),
        name: field_name,
        producing_arg,
        cow_info,
    };

    let iters_states = field.iter_hall.iter_states().collect::<Vec<_>>();

    let mut pending_actions = Vec::new();

    let mut ab = jd.match_set_mgr.match_sets[field.match_set]
        .action_buffer
        .borrow_mut();

    ab.gather_pending_actions(
        &mut pending_actions,
        field.first_actor.get(),
        field.snapshot.get(),
    );

    let cfr = jd.field_mgr.get_cow_field_ref_raw(field_id);
    field_data_to_json(
        jd,
        &cfr,
        field_count_cow,
        &field_info,
        &field.field_refs,
        dead_slots,
        &pending_actions,
        iters_states,
    )
}

fn group_track_iters_to_json(iters: &[Cell<GroupTrackIterState>]) -> Value {
    return Value::Array({
        iters
            .iter()
            .filter_map(|i| {
                let it = i.get();
                iter_kind_to_json(
                    &it.kind,
                    &format!(
                        "GID: {}, GOFF: {}, FP: {}",
                        it.group_idx, it.group_offset, it.field_pos
                    ),
                )
            })
            .collect::<Vec<_>>()
    });
}

fn group_track_to_json(
    jd: &JobData,
    group_track_id: GroupTrackId,
    dead_slots: &[usize],
) -> serde_json::Value {
    jd.group_track_manager
        .apply_actions_to_track(&jd.match_set_mgr, group_track_id);
    let mut gt =
        jd.group_track_manager.group_tracks[group_track_id].borrow_mut();
    gt.sort_iters();
    let mut iter = gt.iter();
    let mut rows = Vec::new();

    let iters_before_start = gt
        .iter_states
        .iter()
        .position(|i| i.get().field_pos > gt.passed_fields_count)
        .unwrap_or(gt.iter_states.len());
    let mut iters_start = iters_before_start;
    let mut passed_count_rem = gt.passed_fields_count;
    let mut is_next_group_start = true;
    let mut group_len = 0;

    let mut del_count = 0;

    loop {
        let passed = passed_count_rem > 0;
        let mut dead_slot_count = 0;
        let is_curr_group_start = is_next_group_start;
        let group_idx;
        let parent_group_advancement;
        let group_len_rem;
        let mut iters_end = iters_start;

        if passed {
            dead_slot_count =
                dead_slots[gt.passed_fields_count - passed_count_rem];
            group_idx = -1;
            group_len_rem = passed_count_rem;
            passed_count_rem -= 1;
            parent_group_advancement = 0;
            is_next_group_start = passed_count_rem == 0;
        } else {
            if iter.group_track().group_lengths.is_empty() {
                break;
            }
            group_idx = iter.group_idx_stable().into_usize() as isize;
            parent_group_advancement = iter
                .group_track()
                .parent_group_advancement
                .get(iter.group_idx());

            group_len_rem = iter.group_len_rem();

            loop {
                let Some(it) = gt.iter_states.get(iters_end) else {
                    break;
                };
                if it.get().field_pos > iter.field_pos() + 1 {
                    break;
                }
                iters_end += 1;
            }

            if group_len_rem == 0 {
                del_count += 1;
            } else {
                dead_slot_count =
                    dead_slots.get(iter.field_pos()).copied().unwrap_or(0)
                        - del_count;
                iter.next_n_fields(1);
                del_count = 0;
            }

            is_next_group_start = iter.is_end_of_group(true);
        }

        let row_iters =
            group_track_iters_to_json(&gt.iter_states[iters_start..iters_end]);
        iters_start = iters_end;

        if is_curr_group_start {
            group_len = group_len_rem;
        }

        rows.push(json!({
            "parent_group_advancement": parent_group_advancement,
            "group_idx": group_idx,
            "passed": passed_count_rem,
            "dead_slots": dead_slot_count,
            "group_len": group_len,
            "is_group_start": is_curr_group_start,
            "iters": row_iters,
        }));

        if is_next_group_start && (!passed && !iter.try_next_group()) {
            break;
        }
    }

    json!({
        "id":  group_track_id.into_usize(),
        "rows": rows,
        "iters": group_track_iters_to_json(&gt.iter_states),
        "iters_before_start": group_track_iters_to_json(&gt.iter_states[..iters_before_start]),
        "parent": gt.parent_group_track_id().map(IndexingType::into_usize),
        "alias_source": gt.alias_source.map(IndexingType::into_usize),
        "corresponding_header": gt.corresponding_header.map(IndexingType::into_usize),
    })
}

pub fn write_debug_log_html_head(
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    TEMPLATES
        .render_to_write(
            "head",
            &json!({
                "debug_style_sheet": cfg!(feature="debug_log_extern_style_sheet")
            }),
            BufWriter::new(w),
        )
        .map_err(unwrap_render_error)
}

pub fn write_debug_log_html_tail(
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    TEMPLATES
        .render_to_write("tail", &(), BufWriter::new(w))
        .map_err(unwrap_render_error)
}

pub fn stream_value_data_offset_to_json(
    offs: &StreamValueDataOffset,
) -> Value {
    json!({
        "values_consumed": offs.values_consumed,
        "current_value_offset": offs.current_value_offset,
    })
}

pub fn stream_values_to_json(jd: &JobData) -> serde_json::Value {
    let mut svs_json = Vec::new();
    let mut string_store = LazyRwLockGuard::new(&jd.session_data.string_store);
    let mut formatting_context = FormattingContext {
        ss: Some(&mut string_store),
        fm: Some(&jd.field_mgr),
        msm: Some(&jd.match_set_mgr),
        rationals_print_mode: RationalsPrintMode::Dynamic,
        is_stream_value: false,
        rfk: RealizedFormatKey {
            opts: FormatOptions {
                type_repr: TypeReprFormat::Typed,
                ..FormatOptions::default()
            },
            ..RealizedFormatKey::default()
        },
    };

    for (sv_id, sv) in jd.sv_mgr.stream_values.iter_enumerated() {
        let mut subscribers = Vec::new();
        for sub in &sv.subscribers {
            subscribers.push(json!({
                "tf_id": sub.tf_id.into_usize(),
                "notify_only_once_done": sub.notify_only_once_done,
                "offset": stream_value_data_offset_to_json(&sub.data_offset),
            }));
        }
        let mut sv_data = Vec::new();
        for data in &sv.data {
            let value = 'data_text: {
                let data_raw: MaybeText = match data {
                    StreamValueData::StaticText(v) => (*v).into(),
                    StreamValueData::StaticBytes(v) => (*v).into(),
                    StreamValueData::Text { data, .. } => (&***data).into(),
                    StreamValueData::Bytes { data, .. } => (&***data).into(),
                    StreamValueData::Single(v) => {
                        break 'data_text match v {
                            FieldValue::FieldReference(fr) => MaybeText::Text(
                                format!("{}", fr.field_ref_offset),
                            ),
                            FieldValue::SlicedFieldReference(fr) => {
                                MaybeText::Text(format!(
                                    "({})[{}..{}]",
                                    fr.field_ref_offset, fr.begin, fr.end
                                ))
                            }
                            FieldValue::StreamValueId(sv_id) => {
                                MaybeText::Text(sv_id.to_string())
                            }
                            _ => {
                                let mut value_str = MaybeText::new();
                                Formattable::format(
                                    &v.as_ref(),
                                    &mut formatting_context,
                                    &mut value_str,
                                )
                                .unwrap();
                                value_str
                            }
                        }
                    }
                };
                let mut value_escaped = MaybeText::new();
                Formattable::format(
                    &FieldValue::from_maybe_text(data_raw).as_ref(),
                    &mut formatting_context,
                    &mut value_escaped,
                )
                .unwrap();
                value_escaped
            };
            sv_data.push(json!({
                "kind": data.kind().to_str(),
                "range": data.range(),
                "value": value.into_text_lossy(),
            }));
        }
        svs_json.push(json!({
            "id":  sv_id.into_usize(),
            "buffer_mode": sv.buffer_mode.to_str(),
            "subscribers": subscribers,
            "ref_count": sv.ref_count,
            "done": sv.done,
            "values_dropped": sv.values_dropped,
            "data_consumed": stream_value_data_offset_to_json(&sv.data_consumed),
            "data_type": sv.data_type.map(|dt| dt.kind().to_str()),
            "error":  sv.error.as_ref().map(|e|e.message()),
            "data": sv_data,
        }));
    }
    svs_json.into()
}

pub fn stream_value_updates_to_json(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData<'_>>,
) -> serde_json::Value {
    let mut svus_json = Vec::new();

    for svu in &jd.sv_mgr.updates {
        svus_json.push(json!({
           "tf_id": svu.tf_id.into_usize(),
           "tf_display_name": &*tf_data[svu.tf_id].display_name(),
           "sv_id": svu.sv_id.into_usize(),
           "offset": stream_value_data_offset_to_json(&svu.data_offset),
        }));
    }
    svus_json.into()
}

pub fn write_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    update_text: &str,
    update_kind: &str,
    tf_id: Option<TransformId>,
    root_tf: TransformId,
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    let transform_chain = setup_transform_chain(jd, tf_data, root_tf);

    let update = &json!({
        "transform_update_index": jd.transform_step_count,
        "transform_id": tf_id.map(TransformId::into_usize),
        "transform_update_text": update_text,
        "transform_chain": transform_chain_to_json(jd, tf_data, &transform_chain),
        "stream_values": stream_values_to_json(jd),
        "stream_value_updates": stream_value_updates_to_json(jd, tf_data),
        "update_kind": update_kind
    });

    TEMPLATES
        .render_to_write("update", &update, BufWriter::new(w))
        .map_err(unwrap_render_error)
}

pub fn write_transform_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_id: TransformId,
    batch_size: usize,
    root_tf: TransformId,
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    write_update_to_html(
        jd,
        tf_data,
        &jd.tf_mgr
            .format_transform_state(tf_id, tf_data, Some(batch_size)),
        "transform-update",
        Some(tf_id),
        root_tf,
        w,
    )
}

pub fn write_stream_value_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    svu: StreamValueUpdate,
    root_tf: TransformId,
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    write_update_to_html(
        jd,
        tf_data,
        &format!(
            "sv {} update for tf {:02} `{}`, stack:{:?}",
            svu.sv_id,
            svu.tf_id,
            tf_data[svu.tf_id].display_name(),
            &jd.tf_mgr.ready_stack
        ),
        "stream-value-update",
        Some(svu.tf_id),
        root_tf,
        w,
    )
}

pub fn write_stream_producer_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_id: TransformId,
    root_tf: TransformId,
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    write_update_to_html(
        jd,
        tf_data,
        &format!(
            "stream producer update for tf {:02} `{}`, stack:{:?}",
            tf_id,
            tf_data[tf_id].display_name(),
            &jd.tf_mgr.ready_stack
        ),
        "stream-producer-update",
        Some(tf_id),
        root_tf,
        w,
    )
}

pub fn write_initial_state_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    root_tf: TransformId,
    w: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    write_update_to_html(
        jd,
        tf_data,
        "Initial State",
        "initial-state",
        None,
        root_tf,
        w,
    )
}
