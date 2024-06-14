use std::borrow::Borrow;

use crate::{
    job::JobData,
    operators::transform::{TransformData, TransformId},
    utils::{
        index_vec::IndexSlice, lazy_lock_guard::LazyRwLockGuard,
        maybe_text::MaybeText, string_store::StringStore,
        text_write::TextWrite,
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
    Transform(TransformId),
    #[allow(unused)] // TODO
    SubchainExpansion(Vec<DisplayOrder>),
}

const INDENT: usize = 4;
// used as a workaround for the fact that "\n" doesn't work in raw string
// literals
const NEWLINE: &str = "\n";

#[derive(Default)]
struct DisplayOrder {
    elems: Vec<DisplayElem>,
    dead_slots: Vec<usize>,
}

fn escape_text(text: &str) -> String {
    text.replace('&', "&amp")
        .replace('<', "&lt")
        .replace('>', "&gt")
}

pub fn write_debug_log_html_head(
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_all_text(&reindent(
        0,
        r"
            <html>
                <head>
                    <style>
        ",
    ))?;
    w.write_all_text(&reindent(INDENT * 3, include_str!("./debug_log.css")))?;
    w.write_all_text(&reindent(
        4,
        r"

                </style>
            </head>
            <body>
        ",
    ))
}

pub fn write_debug_log_html_tail(
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_all_text(&reindent(
        INDENT * 2,
        r"
              </body>
            </html>
        ",
    ))
}

pub fn write_transform_update_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    tf_id: TransformId,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    w.write_all_text(&reindent(
        INDENT * 2,
        format!(
            "<div class=\"transform_update\">transform update {}</div>\n",
            escape_text(&jd.tf_mgr.format_transform_state(tf_id, tf_data))
        ),
    ))
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

fn setup_dead_slots(display_order: &mut DisplayOrder, jd: &JobData) {
    for elem in &display_order.elems {
        let DisplayElem::Field(field_id) = elem else {
            continue;
        };
        jd.field_mgr
            .apply_field_actions(&jd.match_set_mgr, *field_id);
        let cfr = jd.field_mgr.get_cow_field_ref_raw(*field_id);
        let fc = cfr.destructured_field_ref().field_count;
        display_order
            .dead_slots
            .resize(display_order.dead_slots.len().max(fc), 0);
        add_field_data_dead_slots(&cfr, &mut display_order.dead_slots[0..fc]);
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
        display_order.elems.push(DisplayElem::Transform(tf_id));
        let tf = &jd.tf_mgr.transforms[tf_id];
        // TODO: handle multiple output fields

        if let TransformData::ForkCat(fc) = &tf_data[tf_id] {
            let mut subchains = Vec::new();
            for sce in &fc.subchains {
                let mut sc_disp_order = DisplayOrder::default();
                push_field_elem_with_refs(
                    jd,
                    jd.tf_mgr.transforms[sce.start_tf].input_field,
                    &mut sc_disp_order,
                );
                setup_display_order_elems(
                    jd,
                    tf_data,
                    sce.start_tf,
                    &mut sc_disp_order,
                );
                setup_dead_slots(&mut sc_disp_order, jd);
                subchains.push(sc_disp_order);
            }
            display_order
                .elems
                .push(DisplayElem::SubchainExpansion(subchains));
            push_field_elem_with_refs(jd, tf.output_field, display_order);
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
}

fn push_field_elem_with_refs(
    jd: &JobData,
    field: FieldId,
    sc_disp_order: &mut DisplayOrder,
) {
    for &fr in &jd.field_mgr.fields[field].borrow().field_refs {
        sc_disp_order.elems.push(DisplayElem::Field(fr));
    }
    sc_disp_order.elems.push(DisplayElem::Field(field));
}

fn write_display_order_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    display_order: &DisplayOrder,
    w: &mut impl TextWrite,
    indent: usize,
) -> Result<(), std::io::Error> {
    w.write_all_text(&reindent(
        INDENT * indent,
        r"
            <table>
                <tbody>
        ",
    ))?;
    for elem in &display_order.elems {
        match elem {
            DisplayElem::Field(field_id) => {
                w.write_all_text(&reindent(
                    INDENT * (indent + 2),
                    "<td class=\"field_list_entry\">\n",
                ))?;
                write_field_to_html_table(
                    jd,
                    &jd.field_mgr.fields[*field_id].borrow(),
                    *field_id,
                    &jd.session_data.string_store.borrow().read().unwrap(),
                    &display_order.dead_slots,
                    indent + 3,
                    w,
                )?;
                w.write_all_text(&reindent(INDENT * (indent + 2), "</td>\n"))?;
            }
            DisplayElem::Transform(tf_id) => {
                w.write_all_text(&reindent(
                    INDENT * (indent + 2),
                    "<td class=\"transform_entry\">\n",
                ))?;
                let tf_data = &tf_data[*tf_id];
                w.write_all_text(&reindent(
                    INDENT * (indent + 3),
                    format!("tf {tf_id} `{}`\n", tf_data.display_name()),
                ))?;
                w.write_all_text(&reindent(INDENT * (indent + 2), "</td>\n"))?;
            }
            DisplayElem::SubchainExpansion(subchains) => {
                w.write_all_text(&reindent(
                    INDENT * (indent + 2),
                    "<td class=\"transform_entry\">\n",
                ))?;
                for sc_display_order in subchains {
                    write_display_order_to_html(
                        jd,
                        tf_data,
                        sc_display_order,
                        w,
                        indent + 3,
                    )?;
                }
                w.write_all_text(&reindent(INDENT * (indent + 2), "</td>\n"))?;
            }
        }
    }
    w.write_all_text(&reindent(
        INDENT * indent,
        r"
                </tbody>
            </table>
        ",
    ))?;
    Ok(())
}

pub fn write_debug_log_to_html(
    jd: &JobData,
    tf_data: &IndexSlice<TransformId, TransformData>,
    start_tf: TransformId,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let mut display_order = DisplayOrder::default();
    setup_display_order_elems(jd, tf_data, start_tf, &mut display_order);
    setup_dead_slots(&mut display_order, jd);
    write_display_order_to_html(jd, tf_data, &display_order, w, 2)
}

pub fn write_field_to_html_table(
    jd: &JobData,
    field: &Field,
    field_id: FieldId,
    string_store: &StringStore,
    dead_slots: &[usize],
    indent: usize,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
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
    write_field_data_to_html_table(
        jd,
        &cfr,
        &field_name,
        &cow_src_str,
        &field.field_refs,
        dead_slots,
        indent,
        w,
    )
}

pub fn reindent(target_ident: usize, input: impl AsRef<str>) -> String {
    fn non_whitespace(b: u8) -> bool {
        b != b' ' && b != b'\t'
    }

    let mut src = input.as_ref();

    // strip leading newline
    if src.starts_with('\n') {
        src = &src[1..];
    }

    // Largest number of spaces that can be removed from every
    // non-whitespace-only line after the first
    let leading_space_count = src
        .lines()
        .filter_map(|line| line.bytes().position(non_whitespace))
        .min()
        .unwrap_or(0);

    let mut result = String::new();
    for (i, line) in src.lines().enumerate() {
        if i != 0 {
            result.push('\n');
        }
        if line.bytes().any(non_whitespace) {
            result.extend(std::iter::repeat(' ').take(target_ident));
            result.push_str(&line[leading_space_count..]);
        };
    }
    if src.ends_with('\n') {
        result.push('\n');
    }
    result
}

pub fn write_field_data_to_html_table<'a>(
    jd: &JobData,
    fd: impl FieldDataRef<'a>,
    heading: &str,
    cow_src_str: &str,
    field_refs: &[FieldId],
    dead_slots: &[usize],
    indent: usize,
    w: &mut impl TextWrite,
) -> Result<(), std::io::Error> {
    let mut iter = super::iters::FieldIter::from_start_allow_dead(&fd);
    w.write_all_text(&reindent(INDENT * indent, format!(
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
    )))?;
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

    while iter.is_next_valid() && iter.field_pos < fd.field_count() {
        let h = fd.headers()[iter.header_idx];
        if h.deleted() {
            del_count += 1;
        } else {
            for _ in del_count..dead_slots[iter.field_pos] {
                w.write_all_text(&reindent(INDENT * 5,
                    "<tr class=\"field_row dead_row\"><td colspan=4></td></tr>\n",
                ))?;
            }
            del_count = 0;
        }
        let shadow_elem = iter.header_rl_offset != 0;
        let flag_shadow = if shadow_elem { " flag_shadow" } else { "" };
        w.write_all_text(&reindent(
            INDENT * (indent + 2),
            r#"
                <tr class="field_row">
                    <td class="field_cell">
                        <table>
                            <tbody>
                                <tr>
            "#,
        ))?;
        w.write_all_text(&reindent(
            INDENT * (indent + 7),
            format!(
                r#"<td class="meta meta_main{flag_shadow}">{}</td>{NEWLINE}"#,
                escape_text(&h.fmt.repr.to_string())
            ),
        ))?;
        w.write_all_text(&reindent(
            INDENT * (indent + 7),
            format!(
                r#"<td class="meta meta_padding{flag_shadow}">{}</td>{NEWLINE}"#,
                h.fmt.leading_padding()
            ),
        ))?;
        w.write_all_text(&reindent(
            INDENT * (indent + 7),
            format!(
                r#"<td class="meta flag {}{flag_shadow}"></td>{NEWLINE}"#,
                if h.same_value_as_previous() {
                    "flag_same_as_prev"
                } else {
                    "flag_disabled"
                }
            ),
        ))?;
        w.write_all_text(&reindent(
            INDENT * (indent + 7),
            format!(
                r#"<td class="meta flag {}{flag_shadow}"></td>{NEWLINE}"#,
                if h.shared_value() {
                    "flag_shared"
                } else {
                    "flag_disabled"
                },
            ),
        ))?;
        w.write_all_text(&reindent(
            INDENT * (indent + 7),
            format!(
                r#"<td class="meta flag {}{flag_shadow}"></td>{NEWLINE}"#,
                if h.deleted() {
                    "flag_deleted"
                } else {
                    "flag_disabled"
                },
            ),
        ))?;
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
        w.write_all_text(&reindent(
            INDENT * (indent + 3),
            format!(
                r#"
                                </tr>
                            </tbody>
                        </table>
                    </td>
                    <td class="size{flag_shadow}">{}</td>
                    <td class="rl{flag_shadow}">{}</td>
                    <td class="data">{}</td>
                "#,
                h.size,
                if shadow_elem {
                    " ".to_string()
                } else {
                    h.run_length.to_string()
                },
                escape_text(&value_str.into_text_lossy()),
            ),
        ))?;

        w.write_all_text(&reindent(INDENT * (indent + 2), "</tr>\n"))?;
        iter.next_field_allow_dead();
    }

    w.write_all_text(&reindent(
        INDENT * indent,
        r"
            </tbody>
        </table>
        ",
    ))?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::record_data::debug_log::reindent;

    #[test]
    fn basic_reindent() {
        assert_eq!("    asdf", reindent(4, "asdf"));
    }

    #[test]
    fn reindent_strips_leading_line() {
        assert_eq!(
            "asdf",
            reindent(
                0,
                "
                asdf
                "
            )
        );
    }

    #[test]
    fn reindent_doesnt_strip_trailing_line() {
        assert_eq!("    asdf\n", reindent(4, "asdf\n"));
        assert_eq!(
            "    asdf\n",
            reindent(
                4,
                "
                asdf
                "
            )
        );
    }
}
