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
        "<div class=\"transform_update\">{tf}</div>\n"
    ))
}

pub fn write_fields_to_html(
    jd: &JobData,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
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
        jd.field_mgr.apply_field_actions(&jd.match_set_mgr, i);
        w.write_all_text("                <td class=\"field_list_entry\">\n")?;
        write_field_to_html_table(
            &f.borrow(),
            i,
            &jd.session_data.string_store.borrow().read().unwrap(),
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
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    write_field_data_to_html_table(
        &field.iter_hall.field_data,
        Some((id as usize, field.name.map(|id| string_store.lookup(id)))),
        w,
    )
}

pub fn write_field_data_to_html_table(
    fd: &FieldData,
    heading: Option<(usize, Option<&str>)>,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let mut iter = fd.iter();
    w.write_text_fmt(format_args!(
        r#"
    <table class="field">
        <thead>
            <th class="field_cell field_desc" colspan="4">Field {}</th>
        </thead>
        <thead>
            <th class="field_cell meta_head">Meta</th>
            <th class="field_cell size_head">Size</th>
            <th class="field_cell rl_head">RL</th>
            <th class="field_cell data_head">Data</th>
        </thead>
        <tbody>
    "#,
        match heading {
            Some((id, None)) => id.to_string(),
            Some((id, Some(name))) => format!("{id} (`{name}`)"),
            None => todo!(),
        },
    ))?;
    let mut header_idx = 0;
    let mut header_offset = 0;
    while header_idx < fd.headers.len() {
        let h = fd.headers[header_idx];
        let shadow_elem = header_offset == 0;
        let flag_shadow = if shadow_elem { "" } else { " flag_shadow" };
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
            h.fmt.repr.to_string()
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
                h.run_length.to_string()
            } else {
                " ".to_string()
            },
            iter.get_next_typed_field()
                .value
                .to_field_value()
                .to_string()
        ))?;

        w.write_all_text(
            r#"
            </tr>
        "#,
        )?;
        header_offset += 1;
        iter.next_field_allow_dead();
        if header_offset >= h.run_length {
            header_idx += 1;
            header_offset = 0;
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
