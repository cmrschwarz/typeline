mod helpers;

use handlebars::{Handlebars, RenderError, RenderErrorReason};
use helpers::reindent;
use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::{
    job::JobData,
    operators::transform::{TransformData, TransformId},
    record_data::{
        field::{Field, FieldId},
        field_value_ref::FieldValueRef,
        formattable::{Formattable, FormattingContext, RealizedFormatKey},
        iter_hall::{CowVariant, IterKind, IterState},
        iters::{FieldDataRef, FieldIter, FieldIterator},
        match_set::MatchSetId,
    },
    utils::{
        index_vec::IndexSlice, indexing_type::IndexingType,
        lazy_lock_guard::LazyRwLockGuard, maybe_text::MaybeText,
        string_store::StringStore,
    },
};

struct MatchChain {
    ms_id: MatchSetId,
    start_tf_id: TransformId,
    tf_envs: Vec<TransformEnv>,
    dead_slots: Vec<usize>,
}

struct TransformEnv {
    tf_id: Option<TransformId>,
    subchains: Vec<TransformChain>,
    fields: Vec<FieldId>,
}

struct TransformChain {
    match_chains: Vec<MatchChain>,
}

#[derive(Clone, Debug)]
pub struct FieldInfo {
    pub id: Option<FieldId>,
    pub name: Option<String>,
    pub producing_arg: Option<String>,
    pub cow_info: Option<CowInfo>,
}

#[derive(Clone, Copy, Debug)]
pub struct CowInfo {
    pub source: Option<FieldId>,
    pub variant: CowVariant,
}

impl MatchChain {
    pub fn new(jd: &JobData, start_tf: TransformId) -> Self {
        Self {
            ms_id: jd.tf_mgr.transforms[start_tf].match_set_id,
            start_tf_id: start_tf,
            tf_envs: Vec::new(),
            dead_slots: Vec::new(),
        }
    }
}

static TEMPLATES: Lazy<Handlebars> = Lazy::new(|| {
    let mut hb = Handlebars::new();
    hb.register_template_string("head", include_str!("head.hbs"))
        .unwrap();
    hb.register_template_string("tail", include_str!("tail.hbs"))
        .unwrap();
    hb.register_partial("field", include_str!("field.hbs"))
        .unwrap();
    hb.register_partial(
        "transform_update",
        include_str!("transform_update.hbs"),
    )
    .unwrap();
    hb.register_partial(
        "transform_chain",
        include_str!("transform_chain.hbs"),
    )
    .unwrap();

    hb.register_helper("unique_id", Box::new(helpers::UniqueId));
    hb.register_helper("repeat", Box::new(helpers::helper_repeat));
    hb.register_helper("let", Box::new(helpers::helper_let));
    hb.register_helper("range", Box::new(helpers::Range));
    hb.register_helper("reindent", Box::new(helpers::Reindent));
    hb.register_helper("stringify", Box::new(helpers::Stringify));
    hb.set_strict_mode(true);
    hb
});

#[allow(clippy::needless_pass_by_value)]
fn unwrap_render_error(te: RenderError) -> std::io::Error {
    match RenderErrorReason::from(te) {
        RenderErrorReason::IOError(e) => e,
        _ => unreachable!(),
    }
}

pub fn write_debug_log_html_head(
    w: impl std::io::Write,
) -> Result<(), std::io::Error> {
    TEMPLATES
        .render_to_write(
            "head",
            &json!({
                "style": include_str!("style.css"),
                "debug_style_sheet": cfg!(feature="debug_debug_log_style_sheet")
            }),
            w,
        )
        .map_err(unwrap_render_error)
}

pub fn write_debug_log_html_tail(
    w: impl std::io::Write,
) -> Result<(), std::io::Error> {
    TEMPLATES
        .render_to_write("tail", &(), w)
        .map_err(unwrap_render_error)
}

fn add_field_data_dead_slots<'a>(
    fd: impl FieldDataRef<'a>,
    dead_slots: &mut [usize],
) {
    let mut iter = FieldIter::from_start_allow_dead(fd);
    for ds in dead_slots {
        *ds = (*ds).max(iter.skip_dead_fields());
        iter.next_field_allow_dead();
    }
}

fn setup_transform_chain_dead_slots(tc: &mut TransformChain, jd: &JobData) {
    for mc in &mut tc.match_chains {
        for env in &mc.tf_envs {
            for field_id in &env.fields {
                jd.field_mgr
                    .apply_field_actions(&jd.match_set_mgr, *field_id);
                let cfr = jd.field_mgr.get_cow_field_ref_raw(*field_id);
                let fc = cfr.destructured_field_ref().field_count();
                mc.dead_slots.resize(mc.dead_slots.len().max(fc), 0);
                add_field_data_dead_slots(&cfr, &mut mc.dead_slots[0..fc]);
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
    let mut tf_id = match_chain.start_tf_id;
    loop {
        let tf = &jd.tf_mgr.transforms[tf_id];
        // TODO: handle multiple output fields
        let mut subchains = Vec::new();
        let mut fields = Vec::new();
        if let TransformData::ForkCat(fc) = &tf_data[tf_id] {
            let fc_cont = fc.continuation_state.lock().unwrap();
            match_chains.push(match_chain);
            match_chain = MatchChain::new(jd, fc_cont.continuation_tf_id);
            for sce in &fc_cont.subchains {
                let mut sc_match_chain = MatchChain::new(jd, sce.start_tf_id);
                let mut input_tf_env = TransformEnv {
                    tf_id: None,
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
            push_field_component_with_refs(jd, tf.output_field, &mut fields);
        } else {
            tf_data[tf_id].get_out_fields(tf, &mut fields);
        }

        match_chain.tf_envs.push(TransformEnv {
            tf_id: Some(tf_id),
            subchains,
            fields,
        });

        if let Some(succ) = tf.successor {
            tf_id = succ;
        } else {
            break;
        }
    }
    match_chains.push(match_chain);
    TransformChain { match_chains }
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
    for tf_env in &match_chain.tf_envs {
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
        envs.push(json!({
            "transform_id": tf_env.tf_id.map(IndexingType::into_usize),
            "transform_display_name": tf_env.tf_id.map(|id|tf_data[id].display_name().to_string()),
            "subchains": subchains,
            "fields": fields
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

fn setup_transform_chain(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
) -> TransformChain {
    let mut tf_chain = setup_transform_chain_tf_envs(
        jd,
        tf_data,
        MatchChain::new(jd, start_tf),
    );
    setup_transform_chain_dead_slots(&mut tf_chain, jd);
    tf_chain
}

pub fn field_data_to_json<'a>(
    jd: &JobData,
    fd: impl FieldDataRef<'a>,
    field_info: &FieldInfo,
    field_refs: &[FieldId],
    dead_slots: &[usize],
    mut iters: Vec<IterState>,
) -> serde_json::Value {
    iters.sort_by(|is1, is2| is1.field_pos.cmp(&is2.field_pos));
    let mut iter = FieldIter::from_start_allow_dead(&fd);

    let mut del_count = 0;
    let mut string_store = LazyRwLockGuard::new(&jd.session_data.string_store);
    let mut formatting_context = FormattingContext {
        ss: &mut string_store,
        fm: &jd.field_mgr,
        msm: &jd.match_set_mgr,
        print_rationals_raw: true,
        is_stream_value: false,
        rfk: RealizedFormatKey::default(),
    };

    let mut rows = Vec::new();

    let mut iters_start = 0;
    while iter.is_next_valid() && iter.get_next_field_pos() < fd.field_count()
    {
        let field_pos = iter.get_next_field_pos();
        while iters
            .get(iters_start)
            .map(|i| i.field_pos < field_pos)
            .unwrap_or(false)
        {
            iters_start += 1;
        }
        let iters_end = iters
            .iter()
            .position(|i| i.field_pos > field_pos)
            .unwrap_or(iters_start);
        let h = fd.headers()[iter.get_next_header_index()];
        let dead_slot_count = if h.deleted() {
            del_count += 1;
            0
        } else {
            let del_count_prev = del_count;
            del_count = 0;
            dead_slots[iter.get_next_field_pos()] - del_count_prev
        };
        let shadow_elem = h.shared_value() && iter.field_run_length_bwd() != 0;
        let flag_shadow = if shadow_elem { " flag_shadow" } else { "" };

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

        let run_length = if iter.field_run_length_bwd() != 0 {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(String::new())
        };

        let row_iters = if h.deleted() {
            Vec::new()
        } else {
            iters[iters_start..iters_end]
                .iter()
                .filter_map(|i| {
                    Some(match i.kind {
                        IterKind::Undefined => json!({
                            "transform_id": Value::Null,
                            "cow_field_id": Value::Null,
                            "display_text": "undef"
                        }),
                        IterKind::Transform(tf_id) => json!({
                            "transform_id": tf_id.into_usize(),
                            "cow_field_id": Value::Null,
                            "display_text": format!("tf {tf_id}")
                        }),
                        IterKind::CowField(cow_field_id) => json!({
                            "transform_id": Value::Null,
                            "cow_field_id": cow_field_id,
                            "display_text": format!("cow field {cow_field_id}")
                        }),
                        IterKind::RefLookup => return None,
                    })
                })
                .collect::<Vec<_>>()
        };

        iters_start = iters_end;

        rows.push(json!({
            "dead_slots": dead_slot_count,
            "flag_shadow": flag_shadow,
            "repr": h.fmt.repr.to_string(),
            "padding": h.fmt.leading_padding(),
            "deleted": h.deleted(),
            "shared": h.shared_value(),
            "size": h.size,
            "run_length": run_length,
            "same_as_prev": h.same_value_as_previous(),
            "value": value_str.into_text().unwrap(),
            "iters": row_iters
        }));

        iter.next_field_allow_dead();
    }

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
            "source": info.source,
        })
    } else {
        Value::Null
    };

    json!({
        "id": field_info.id,
        "name": field_info.name,
        "producing_arg": field_info.producing_arg,
        "cow": cow,
        "field_refs": field_refs,
        "rows": rows,
    })
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

    let mut producing_arg = None;
    #[cfg(feature = "debug_logging")]
    if !field.producing_transform_arg.is_empty() {
        producing_arg = Some(field.producing_transform_arg.to_string());
    }

    let field_info = FieldInfo {
        id: Some(field_id),
        name: field.name.map(|id| string_store.lookup(id).to_string()),
        producing_arg,
        cow_info,
    };

    let iters_states = field.iter_hall.iter_states().collect::<Vec<_>>();

    let cfr = jd.field_mgr.get_cow_field_ref_raw(field_id);
    field_data_to_json(
        jd,
        &cfr,
        &field_info,
        &field.field_refs,
        dead_slots,
        iters_states,
    )
}

pub fn write_transform_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_id: TransformId,
    root_tf: TransformId,
    mut w: impl std::io::Write,
) -> Result<(), std::io::Error> {
    let transform_chain = setup_transform_chain(jd, tf_data, root_tf);

    let update = &json!({
        "transform_update_text": jd.tf_mgr.format_transform_state(tf_id, tf_data),
        "transform_chain": transform_chain_to_json(jd, tf_data, &transform_chain),
    });
    let tf_update = TEMPLATES.render("transform_update", &update).unwrap();
    w.write_all(reindent(false, 8, tf_update).as_bytes())
}
