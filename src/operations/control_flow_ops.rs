use std::collections::HashMap;

use bstring::bstr;

use crate::{
    chain::ChainId,
    options::{argument::CliArgIdx, range_spec::RangeSpec},
};

use super::operator_base::{OperatorCreationError, OperatorId};

#[derive(Clone)]
pub struct OpSplit {
    pub range_spec: RangeSpec<ChainId>,
    pub target_operators: Vec<OperatorId>,
}

pub fn parse_split_op(
    value: Option<&bstr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OpSplit, OperatorCreationError> {
    let range_spec = if let Some(value) = value {
        RangeSpec::<ChainId>::parse(value.to_str().map_err(|_| {
            OperatorCreationError::new(
                "failed to parse split argument as range spec: invalid UTF-8",
                arg_idx,
            )
        })?)
        .map_err(|_| {
            OperatorCreationError::new("failed to parse split argument as range spec", arg_idx)
        })?
    } else {
        RangeSpec::Bounded(Some(0), None)
    };
    Ok(OpSplit {
        range_spec,
        target_operators: Default::default(),
    })
}
