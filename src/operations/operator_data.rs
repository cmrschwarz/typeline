use super::{format::OpFormat, regex::OpRegex, split::OpSplit};

pub enum OperatorData {
    Print,
    Split(OpSplit),
    Regex(OpRegex),
    Format(OpFormat),
}
