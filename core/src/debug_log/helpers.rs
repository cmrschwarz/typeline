use std::sync::atomic::AtomicU64;

use handlebars::{
    handlebars_helper, BlockParamHolder, Context, Handlebars, Helper, Output,
    RenderContext, RenderError, RenderErrorReason, Renderable,
};
use once_cell::sync::Lazy;
use serde_json::Value;

handlebars_helper!(Range: |n: u64| {
    serde_json::Value::Array((0..n).map(
        |n|serde_json::Value::Number(n.into())
    ).collect::<Vec<_>>())
});
handlebars_helper!(Reindent: |n: usize, s: String| {
    reindent(true, n, s)
});
static UNIQUE_ID_COUNTER: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));
handlebars_helper!(UniqueId: |prefix: String| {
    let mut prefix = prefix;
    prefix.push_str(
        &UNIQUE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst).to_string()
    );
    prefix
});

handlebars_helper!(Stringify: |object: Value| {
    format!("{object:#?}")
});

pub fn reindent(
    skip_first: bool,
    target_ident: usize,
    input: impl AsRef<str>,
) -> String {
    fn non_whitespace(b: u8) -> bool {
        b != b' ' && b != b'\t'
    }
    let src = input.as_ref();

    // Largest number of spaces that can be removed from every
    // non-whitespace-only line after the first
    let leading_space_count = src
        .lines()
        .filter_map(|line| line.bytes().position(non_whitespace))
        .min()
        .unwrap_or(0);

    let mut result = String::new();
    for (i, line) in src.lines().enumerate() {
        if i == 0 && skip_first {
            result.push_str(line);
            continue;
        }
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

// a custom block helper to repeat a block n times
pub fn helper_repeat<'reg, 'rc>(
    h: &Helper<'rc>,
    r: &'reg Handlebars<'reg>,
    ctx: &'rc Context,
    rc: &mut RenderContext<'reg, 'rc>,
    out: &mut dyn Output,
) -> Result<(), RenderError> {
    let count = h
        .param(0)
        .as_ref()
        .and_then(|v| v.value().as_u64())
        .ok_or_else(|| {
            RenderErrorReason::ParamTypeMismatchForName(
                "repeat",
                "count".to_string(),
                "u64".to_string(),
            )
        })?;

    let template = h
        .template()
        .ok_or(RenderErrorReason::BlockContentRequired)?;

    for _ in 0..count {
        template.render(r, ctx, rc, out)?;
    }

    rc.pop_block();

    Ok(())
}

// a custom block helper to repeat a block n times
pub fn helper_let<'reg, 'rc>(
    h: &Helper<'rc>,
    _r: &'reg Handlebars<'reg>,
    _ctx: &'rc Context,
    rc: &mut RenderContext<'reg, 'rc>,
    _out: &mut dyn Output,
) -> Result<(), RenderError> {
    let name_param = h
        .param(0)
        .ok_or_else(|| RenderErrorReason::ParamNotFoundForIndex("let", 0))?;

    let handlebars::ScopedJson::Constant(Value::String(name_constant)) =
        name_param.value
    else {
        return Err(RenderErrorReason::ParamTypeMismatchForName(
            "let",
            "0".to_string(),
            "constant string".to_string(),
        )
        .into());
    };

    let value = h
        .param(1)
        .as_ref()
        .map(|v| v.value().to_owned())
        .ok_or_else(|| RenderErrorReason::ParamNotFoundForIndex("let", 2))?;

    let block = rc.block_mut().unwrap();

    block.set_block_param(name_constant, BlockParamHolder::Value(value));

    Ok(())
}

#[cfg(test)]
mod test {
    use super::reindent;

    #[test]
    fn basic_reindent() {
        assert_eq!("    asdf", reindent(false, 4, "asdf"));
    }

    #[test]
    fn multiline_reindent() {
        assert_eq!("      asdf\n    qq", reindent(false, 4, "  asdf\nqq"));
    }
}
