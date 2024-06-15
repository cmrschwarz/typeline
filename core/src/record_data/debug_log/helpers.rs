use handlebars::handlebars_helper;

handlebars_helper!(Range: |n: u64| {
    serde_json::Value::Array((0..n).map(
        |n|serde_json::Value::Number(n.into())
    ).collect::<Vec<_>>())
});
handlebars_helper!(Reindent: |n: usize, s: String| {
    reindent(true, n, s)
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
