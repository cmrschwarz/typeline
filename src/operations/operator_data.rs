use super::{control_flow_ops::OpSplit, format::OpFormat, regex::OpRegex};

pub enum OperatorData {
    Print,
    Split(OpSplit),
    Regex(OpRegex),
    Format(OpFormat),
}
