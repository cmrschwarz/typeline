use regex::Regex;

use crate::{chain::ChainId, context::SessionData};

use super::{
    control_flow_ops::OpSplit,
    format::{OpFormat, TfFormat},
    operator_base::OperatorId,
    regex::OpRegex,
};

pub enum OperatorData {
    Print,
    Split(OpSplit),
    Regex(OpRegex),
    Format(OpFormat),
}
