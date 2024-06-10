use std::borrow::Borrow;

use crate::{
    job::JobData,
    operators::transform::{TransformData, TransformId},
    utils::{
        index_vec::IndexSlice, string_store::StringStore,
        text_write::TextWrite,
    },
};

use super::{
    field::{Field, FieldId, FieldManager},
    field_data::FieldData,
    iters::FieldIterator,
};

enum DisplayElem {
    Field(FieldId),
    Transform(TransformId),
    #[allow(unused)] //TODO
    SubchainExpansion(Vec<DisplayOrder>),
}

struct DisplayOrder {
    elems: Vec<DisplayElem>,
    dead_slots: Vec<usize>,
}

fn escape_text(text: &str) -> String {
    text.replace("&", "&amp")
        .replace('<', "&lt")
        .replace(">", "&gt")
}

pub fn write_debug_log_html_head(
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_text_fmt(format_args!(
        r#"
<html>
<head>
    <style>
    {}
    </style>
</head>
<body>
"#,
        include_str!("./debug_log.css"),
    ))
}

pub fn write_debug_log_html_tail(
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_all_text(
        r#"
</body>
</html>
"#,
    )
}

pub fn write_transform_update_to_html(
    tf: &str,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_text_fmt(format_args!(
        "<div class=\"transform_update\">{}</div>\n",
        escape_text(tf)
    ))
}

fn add_field_data_dead_slots(fd: &FieldData, dead_slots: &mut [usize]) {
    let mut iter = super::iters::FieldIter::from_start_allow_dead(fd);
    for i in 0..dead_slots.len() {
        dead_slots[i] = dead_slots[i].max(iter.skip_dead_fields());
        iter.next_field_allow_dead();
    }
}

fn setup_dead_slots(display_order: &mut DisplayOrder, jd: &JobData) {
    for elem in &display_order.elems {
        let DisplayElem::Field(field_id) = elem else {
            continue;
        };
        jd.field_mgr
            .apply_field_actions(&jd.match_set_mgr, *field_id);
        let f = jd.field_mgr.fields[*field_id].borrow();
        let fc = f.iter_hall.field_data.field_count;
        display_order
            .dead_slots
            .resize(display_order.dead_slots.len().max(fc), 0);
        add_field_data_dead_slots(
            &f.iter_hall.field_data,
            &mut display_order.dead_slots[0..fc],
        );
    }
}

fn setup_display_order(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
) -> DisplayOrder {
    let mut fields_temp = Vec::new();
    let mut display_order = DisplayOrder {
        elems: Vec::new(),
        dead_slots: Vec::new(),
    };
    let mut tf_id = start_tf;
    loop {
        display_order.elems.push(DisplayElem::Transform(tf_id));
        let tf = &jd.tf_mgr.transforms[tf_id];
        //TODO: handle multiple output fields

        if let TransformData::ForkCat(fc) = &tf_data[tf_id] {
            let mut subchains = Vec::new();
            for sce in &fc.subchains {
                subchains.push(setup_display_order(jd, tf_data, sce.start_tf));
            }
            display_order
                .elems
                .push(DisplayElem::SubchainExpansion(subchains));
        } else {
            tf_data[tf_id].get_out_fields(tf, &mut fields_temp);
            for &field in &fields_temp {
                display_order.elems.push(DisplayElem::Field(field));
            }
            fields_temp.clear();
        }
        if let Some(succ) = tf.successor {
            tf_id = succ;
        } else {
            break;
        }
    }
    setup_dead_slots(&mut display_order, jd);
    display_order
}

fn write_display_order_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    display_order: &DisplayOrder,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_text_fmt(format_args!(
        r#"
    <table>
        <tbody>
    "#
    ))?;
    for elem in &display_order.elems {
        match elem {
            DisplayElem::Field(field_id) => {
                w.write_all_text(
                    "                <td class=\"field_list_entry\">\n",
                )?;
                write_field_to_html_table(
                    &jd.field_mgr,
                    &jd.field_mgr.fields[*field_id].borrow(),
                    *field_id,
                    &jd.session_data.string_store.borrow().read().unwrap(),
                    &display_order.dead_slots,
                    w,
                )?;
                w.write_all_text("                </td>\n")?;
            }
            DisplayElem::Transform(tf_id) => {
                w.write_all_text(
                    "                <td class=\"transform_entry\">\n",
                )?;
                let tf_data = &tf_data[*tf_id];
                w.write_text_fmt(format_args!(
                    "tf {tf_id} `{}`",
                    tf_data.display_name()
                ))?;
                w.write_all_text("                </td>\n")?;
            }
            DisplayElem::SubchainExpansion(subchains) => {
                w.write_all_text(
                    "                <td class=\"transform_entry\">\n",
                )?;
                for sc_display_order in subchains {
                    write_display_order_to_html(
                        jd,
                        tf_data,
                        sc_display_order,
                        w,
                    )?;
                }
                w.write_all_text("                </td>\n")?;
            }
        }
    }
    w.write_all_text(
        r#"
        </tbody>
    </table>
    "#,
    )?;
    Ok(())
}

pub fn write_debug_log_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let display_order = setup_display_order(jd, tf_data, start_tf);
    write_display_order_to_html(jd, tf_data, &display_order, w)
}

pub fn write_field_to_html_table(
    fm: &FieldManager,
    field: &Field,
    id: FieldId,
    string_store: &StringStore,
    dead_slots: &[usize],
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let field_name = {
        let id = id;
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
        field.iter_hall.cow_source_field(fm)
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
        "".to_string()
    };
    write_field_data_to_html_table(
        &field.iter_hall.field_data,
        &field_name,
        &cow_src_str,
        &field.field_refs,
        dead_slots,
        w,
    )
}

pub fn write_field_data_to_html_table(
    fd: &FieldData,
    heading: &str,
    cow_src_str: &str,
    field_refs: &[FieldId],
    dead_slots: &[usize],
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let mut iter = super::iters::FieldIter::from_start_allow_dead(fd);
    w.write_text_fmt(format_args!(
        r#"
    <table class="field">
        <thead>
            <th class="field_cell field_desc" colspan="4">{}</th>
        </thead>
        <thead>
            <th class="field_cell field_desc" colspan="4">{cow_src_str}Field Refs: {field_refs:?}</th>
        </thead>
        <thead>
            <th class="field_cell meta_head">Meta</th>
            <th class="field_cell size_head">Size</th>
            <th class="field_cell rl_head">RL</th>
            <th class="field_cell data_head">Data</th>
        </thead>
        <tbody>
    "#,
        escape_text(heading)
    ))?;
    let mut del_count = 0;

    while iter.is_next_valid() {
        let h = fd.headers[iter.header_idx];
        if h.deleted() {
            del_count += 1;
        } else {
            for _ in del_count..dead_slots[iter.field_pos] {
                w.write_all_text(
                    "<tr class=\"field_row dead_row\"><td colspan=4></td></tr>\n",
                )?;
            }
            del_count = 0;
        }
        let shadow_elem = iter.header_rl_offset != 0;
        let flag_shadow = if shadow_elem { " flag_shadow" } else { "" };
        w.write_all_text(
            r#"
            <tr class="field_row">
                <td class="field_cell">
                    <table>
                        <tbody>
                            <tr>
        "#,
        )?;
        w.write_text_fmt(format_args!(
            r#"
                                <td class="meta meta_main{flag_shadow}">{}</td>
        "#,
            escape_text(&h.fmt.repr.to_string())
        ))?;
        w.write_text_fmt(format_args!(
            r#"
                                <td class="meta meta_padding{flag_shadow}">{}</td>
        "#,
            h.fmt.leading_padding()
        ))?;
        w.write_text_fmt(format_args!(
            r#"
                                <td class="meta flag {}{flag_shadow}"></td>
        "#,
            if h.same_value_as_previous() {
                "flag_same_as_prev"
            } else {
                "flag_disabled"
            }
        ))?;
        w.write_text_fmt(format_args!(
            r#"
                                <td class="meta flag {}{flag_shadow}"></td>
        "#,
            if h.shared_value() {
                "flag_shared"
            } else {
                "flag_disabled"
            }
        ))?;
        w.write_text_fmt(format_args!(
            r#"
                                <td class="meta flag {}{flag_shadow}"></td>
        "#,
            if h.deleted() {
                "flag_deleted"
            } else {
                "flag_disabled"
            }
        ))?;
        w.write_text_fmt(format_args!(
            r#"
                            </tr>
                        </tbody>
                    </table>
                </td>
                <td class="size {flag_shadow}">{}
                <td class="rl {flag_shadow}">{}
                <td class="data">{}
        "#,
            h.size,
            if shadow_elem {
                " ".to_string()
            } else {
                h.run_length.to_string()
            },
            escape_text(
                &iter
                    .get_next_typed_field()
                    .value
                    .to_field_value()
                    .to_string()
            )
        ))?;

        w.write_all_text(
            r#"
            </tr>
        "#,
        )?;
        iter.next_field_allow_dead();
    }

    w.write_all_text(
        r#"
        </tbody>
    </table>
    "#,
    )?;

    Ok(())
}
