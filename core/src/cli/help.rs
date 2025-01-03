use crate::{
    operators::errors::OperatorCreationError, typeline_error::TypelineError,
};

use super::call_expr::Span;

macro_rules! help_page {
    ($name: literal) => {
        include_str!(concat!("help_sections/", $name))
    };
}

pub fn get_help_page(
    page: Option<(&[u8], Span)>,
    start_span: Span,
) -> Result<&'static str, TypelineError> {
    const MAIN_HELP_PAGE: &str = help_page!("main.txt");
    let section = if let Some((help_section_arg, span)) = page {
        let section_name = String::from_utf8_lossy(help_section_arg);
        match section_name.trim().to_lowercase().as_ref() {
            "cast" => help_page!("cast.txt"),
            "format" | "f" => help_page!("format.txt"),
            "help" | "h" => help_page!("help.txt"),
            "join" | "j" => help_page!("join.txt"),
            "main" => MAIN_HELP_PAGE,
            "print" | "p" => help_page!("print.txt"),
            "regex" | "r" => help_page!("regex.txt"),
            "types" | "int" | "str" | "~str" | "bytes" | "~bytes"
            | "error" | "~error" | "null" | "undefined" | "array"
            | "object" | "integer" | "float" | "rational" => {
                help_page!("types.txt")
            }
            _ => {
                return Err(OperatorCreationError {
                    message: format!("no help section for '{section_name}'")
                        .into(),
                    span: start_span.span_until(span).unwrap(),
                }
                .into())
            }
        }
    } else {
        MAIN_HELP_PAGE
    };
    Ok(section)
}
