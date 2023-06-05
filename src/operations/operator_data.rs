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

pub enum TransformData<'a> {
    Print,
    Split(&'a [ChainId]),
    Regex(Regex),
    Format(TfFormat<'a>),
}

fn setup_transform_data<'a>(
    sd: &'a SessionData,
    chain: ChainId,
    vals: impl Iterator<Item = OperatorId>,
) -> impl Iterator<Item = TransformData<'a>> {
    vals.map(move |op_id| match &sd.operator_data[op_id as usize] {
        OperatorData::Print => TransformData::Print,
        OperatorData::Split(sd) => TransformData::Split(&sd.target_chains[&chain]),
        OperatorData::Regex(rd) => TransformData::Regex(rd.regex.clone()),
        OperatorData::Format(_) => todo!(),
    })
}
