use std::borrow::Borrow;

use crate::{
    job::JobData,
    utils::{string_store::StringStore, text_write::TextWrite},
};

use super::{
    field::{Field, FieldId, VOID_FIELD_ID},
    field_data::FieldData,
    iters::FieldIterator,
};

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

pub fn write_fields_to_html(
    jd: &JobData,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let mut dead_slots = Vec::<usize>::new();
    for (i, f) in jd.field_mgr.fields.iter_enumerated() {
        if i == VOID_FIELD_ID {
            continue;
        }
        jd.field_mgr.apply_field_actions(&jd.match_set_mgr, i);
        let f = f.borrow();
        let fc = f.iter_hall.field_data.field_count;
        dead_slots.resize(dead_slots.len().max(fc), 0);
        add_field_data_dead_slots(
            &f.iter_hall.field_data,
            &mut dead_slots[0..fc],
        );
    }

    w.write_text_fmt(format_args!(
        r#"
    <table>
        <tbody>
    "#
    ))?;
    for (i, f) in jd.field_mgr.fields.iter_enumerated() {
        if i == VOID_FIELD_ID {
            continue;
        }

        w.write_all_text("                <td class=\"field_list_entry\">\n")?;
        write_field_to_html_table(
            &f.borrow(),
            i,
            &jd.session_data.string_store.borrow().read().unwrap(),
            &dead_slots,
            w,
        )?;
        w.write_all_text("                </td>\n")?;
    }
    w.write_all_text(
        r#"
        </tbody>
    </table>
    "#,
    )?;
    Ok(())
}

pub fn write_field_to_html_table(
    field: &Field,
    id: FieldId,
    string_store: &StringStore,
    dead_slots: &[usize],
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let field_name = {
        let id = id;
        let given_name = field.name.map(|id| string_store.lookup(id));
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
    write_field_data_to_html_table(
        &field.iter_hall.field_data,
        &field_name,
        dead_slots,
        w,
    )
}

pub fn write_field_data_to_html_table(
    fd: &FieldData,
    heading: &str,
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
