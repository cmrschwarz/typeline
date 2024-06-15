mod helpers;

use std::iter;

use handlebars::{Handlebars, RenderError, RenderErrorReason};
use helpers::reindent;
use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::{
    job::JobData,
    operators::transform::{TransformData, TransformId},
    utils::{
        index_vec::IndexSlice, indexing_type::IndexingType,
        lazy_lock_guard::LazyRwLockGuard, maybe_text::MaybeText,
        string_store::StringStore,
    },
};

use super::{
    field::{Field, FieldId},
    field_value_ref::FieldValueRef,
    formattable::{Formattable, FormattingContext, RealizedFormatKey},
    iters::{FieldDataRef, FieldIterator},
};

enum DisplayElem {
    Field(FieldId),
    Transform(Option<TransformId>, Vec<DisplayElem>),
    SubchainExpansion(Vec<DisplayOrder>),
}

#[derive(Default)]
struct DisplayOrder {
    elems: Vec<DisplayElem>,
    dead_slots: Vec<usize>,
}

static TEMPLATES: Lazy<Handlebars> = Lazy::new(|| {
    let mut hb = Handlebars::new();
    hb.register_template_string("head", include_str!("head.hbs"))
        .unwrap();
    hb.register_template_string("tail", include_str!("tail.hbs"))
        .unwrap();
    hb.register_partial("field", include_str!("field.hbs"))
        .unwrap();
    hb.register_partial("display_order", include_str!("display_order.hbs"))
        .unwrap();
    hb.register_partial(
        "display_elem_list",
        include_str!("display_elem_list.hbs"),
    )
    .unwrap();
    hb.register_template_string(
        "transform_update",
        include_str!("transform_update.hbs"),
    )
    .unwrap();
    hb.register_helper("unique_id", Box::new(helpers::UniqueId));
    hb.register_helper("range", Box::new(helpers::Range));
    hb.register_helper("reindent", Box::new(helpers::Reindent));
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
                "style": include_str!("style.css")
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

pub fn write_transform_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_id: TransformId,
    mut w: impl std::io::Write,
) -> Result<(), std::io::Error> {
    let tf_update = TEMPLATES.render(
        "transform_update",
        &json!({
            "transform_state": jd.tf_mgr.format_transform_state(tf_id, tf_data)
        }),
    ).unwrap();
    w.write_all(reindent(false, 8, tf_update).as_bytes())
}
fn add_field_data_dead_slots<'a>(
    fd: impl FieldDataRef<'a>,
    dead_slots: &mut [usize],
) {
    let mut iter = super::iters::FieldIter::from_start_allow_dead(fd);
    for ds in dead_slots {
        *ds = (*ds).max(iter.skip_dead_fields());
        iter.next_field_allow_dead();
    }
}

fn setup_dead_slots(
    elems: &[DisplayElem],
    dead_slots: &mut Vec<usize>,
    jd: &JobData,
) {
    for elem in elems {
        match elem {
            DisplayElem::Field(field_id) => {
                jd.field_mgr
                    .apply_field_actions(&jd.match_set_mgr, *field_id);
                let cfr = jd.field_mgr.get_cow_field_ref_raw(*field_id);
                let fc = cfr.destructured_field_ref().field_count;
                dead_slots.resize(dead_slots.len().max(fc), 0);
                add_field_data_dead_slots(&cfr, &mut dead_slots[0..fc]);
            }
            DisplayElem::Transform(_, sub_elems) => {
                setup_dead_slots(sub_elems, dead_slots, jd)
            }
            DisplayElem::SubchainExpansion(_) => (),
        }
    }
}

fn setup_display_order_elems(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
    display_order: &mut DisplayOrder,
) {
    let mut fields_temp = Vec::new();

    let mut tf_id = start_tf;
    loop {
        let tf = &jd.tf_mgr.transforms[tf_id];
        // TODO: handle multiple output fields

        let mut child_elems = Vec::new();

        if let TransformData::ForkCat(fc) = &tf_data[tf_id] {
            let mut subchains = Vec::new();
            for sce in &fc.continuation_state.lock().unwrap().subchains {
                let mut sc_disp_order = DisplayOrder::default();

                push_field_elem_with_refs(
                    jd,
                    jd.tf_mgr.transforms[sce.start_tf_id].input_field,
                    &mut sc_disp_order.elems,
                    true,
                );
                setup_display_order_elems(
                    jd,
                    tf_data,
                    sce.start_tf_id,
                    &mut sc_disp_order,
                );
                setup_dead_slots(
                    &sc_disp_order.elems,
                    &mut sc_disp_order.dead_slots,
                    jd,
                );
                subchains.push(sc_disp_order);
            }
            child_elems.push(DisplayElem::SubchainExpansion(subchains));
            push_field_elem_with_refs(
                jd,
                tf.output_field,
                &mut child_elems,
                false,
            );
        } else {
            tf_data[tf_id].get_out_fields(tf, &mut fields_temp);
            for &field in &fields_temp {
                child_elems.push(DisplayElem::Field(field));
            }
            fields_temp.clear();
        }

        display_order
            .elems
            .push(DisplayElem::Transform(Some(tf_id), child_elems));

        if let Some(succ) = tf.successor {
            tf_id = succ;
        } else {
            break;
        }
    }
}

fn push_field_elem_with_refs(
    jd: &JobData,
    field: FieldId,
    elems: &mut Vec<DisplayElem>,
    wrap_in_empty_transform: bool,
) {
    for &fr in jd.field_mgr.fields[field]
        .borrow()
        .field_refs
        .iter()
        .chain(iter::once(&field))
    {
        if wrap_in_empty_transform {
            elems.push(DisplayElem::Transform(
                None,
                vec![DisplayElem::Field(fr)],
            ));
        } else {
            elems.push(DisplayElem::Field(fr));
        }
    }
}

fn display_elems_to_json(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    display_elems: &[DisplayElem],
    dead_slots: &[usize],
) -> serde_json::Value {
    let mut res = Vec::new();
    let string_store = jd.session_data.string_store.read().unwrap();
    for elem in display_elems {
        match elem {
            DisplayElem::Field(field_id) => res.push(json!({
                "field": field_to_json(
                    jd,
                    &jd.field_mgr.fields[*field_id].borrow(),
                    *field_id,
                    &string_store,
                    dead_slots
                )
            })),
            DisplayElem::Transform(tf_id, sub_elems) => res.push(json!({
                "transform": {
                    "id": tf_id.map(|v| v.into_usize().into()).unwrap_or(Value::Null),
                    "display_name": tf_id.map(|id|tf_data[id].display_name().to_string().into()).unwrap_or(Value::Null),
                    "elements": display_elems_to_json(jd, tf_data, sub_elems, dead_slots),
                }
            })),
            DisplayElem::SubchainExpansion(subchains) => res.push(json!({
                "subchains":
                subchains.iter().map(
                    |display_order| json!({
                        "elements": display_elems_to_json(jd, tf_data, &display_order.elems, &display_order.dead_slots)
                    })
                ).collect::<Vec<_>>()
            })),
        }
    }
    res.into()
}

pub fn write_debug_log_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
    mut w: impl std::io::Write,
) -> Result<(), std::io::Error> {
    let mut display_order = DisplayOrder::default();
    setup_display_order_elems(jd, tf_data, start_tf, &mut display_order);
    setup_dead_slots(&display_order.elems, &mut display_order.dead_slots, jd);
    let display_order_json = display_elems_to_json(
        jd,
        tf_data,
        &display_order.elems,
        &display_order.dead_slots,
    );
    let entry = TEMPLATES
        .render("display_order", &json!({ "elements": display_order_json }))
        .unwrap();
    w.write_all(reindent(false, 8, entry).as_bytes())?;
    Ok(())
}

pub fn field_to_json(
    jd: &JobData,
    field: &Field,
    field_id: FieldId,
    string_store: &StringStore,
    dead_slots: &[usize],
) -> serde_json::Value {
    let field_name = {
        let id = field_id;
        let given_name = field.name.map(|id| string_store.lookup(id));
        #[allow(unused_mut)]
        let mut res = if let Some(name) = given_name {
            format!("Field {id} '{name}'")
        } else {
            format!("Field {id}")
        };
        #[cfg(feature = "debug_logging")]
        {
            let tf_name = &field.producing_transform_arg;
            res = format!("{res} (`{tf_name}`)");
        }
        res
    };
    let cow_src_str = if let (cow_src_field, Some(data_cow)) =
        field.iter_hall.cow_source_field(&jd.field_mgr)
    {
        format!(
            " {} cow{}; ",
            if data_cow { "data" } else { "full" },
            if let Some(src) = cow_src_field {
                format!(" src: {src}")
            } else {
                String::default()
            }
        )
    } else {
        String::new()
    };
    let cfr = jd.field_mgr.get_cow_field_ref_raw(field_id);
    field_data_to_json(
        jd,
        &cfr,
        &field_name,
        &cow_src_str,
        &field.field_refs,
        dead_slots,
    )
}

pub fn field_data_to_json<'a>(
    jd: &JobData,
    fd: impl FieldDataRef<'a>,
    heading: &str,
    cow_src_str: &str,
    field_refs: &[FieldId],
    dead_slots: &[usize],
) -> serde_json::Value {
    let mut iter = super::iters::FieldIter::from_start_allow_dead(&fd);

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

    while iter.is_next_valid() && iter.field_pos < fd.field_count() {
        let h = fd.headers()[iter.header_idx];
        let dead_slot_count = if h.deleted() {
            del_count += 1;
            0
        } else {
            let del_count_prev = del_count;
            del_count = 0;
            dead_slots[iter.field_pos] - del_count_prev
        };
        let shadow_elem = iter.header_rl_offset != 0;
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
            "value": value_str.into_text().unwrap()
        }));

        iter.next_field_allow_dead();
    }

    json!({
        "description": heading,
        "cow_src_str": cow_src_str,
        "field_refs": field_refs,
        "rows": rows
    })
}

pub fn write_field_data_to_html_table<'a>(
    jd: &JobData,
    fd: impl FieldDataRef<'a>,
    heading: &str,
    cow_src_str: &str,
    field_refs: &[FieldId],
    dead_slots: &[usize],
    w: impl std::io::Write,
) -> Result<(), std::io::Error> {
    let field_data = field_data_to_json(
        jd,
        fd,
        heading,
        cow_src_str,
        field_refs,
        dead_slots,
    );
    TEMPLATES
        .render_template_to_write("field", &field_data, w)
        .map_err(unwrap_render_error)
}
