pub fn int_sequence_strings(count: usize) -> Vec<String> {
    (0..count).into_iter().map(|n| n.to_string()).collect()
}

pub fn int_sequence_newline_separated(count: usize) -> String {
    int_sequence_strings(count)
        .iter()
        .fold(String::new(), |mut f, n| {
            f.push_str(n.to_string().as_str());
            f.push_str("\n");
            f
        })
}
