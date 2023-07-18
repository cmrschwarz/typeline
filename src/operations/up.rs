use std::num::NonZeroUsize;

use crate::options::argument::CliArgIdx;

use super::{errors::OperatorCreationError, operator::OperatorData};

#[derive(Clone)]
pub struct OpUp {
    pub step: NonZeroUsize,
}

pub fn parse_op_up(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let step = value
        .map(|step| {
            std::str::from_utf8(step)
                .map_err(|_| {
                    OperatorCreationError::new(
                        "failed to parse argument as integer (invalid UTF-8)",
                        arg_idx,
                    )
                })?
                .parse::<i64>()
                .map_err(|_| {
                    OperatorCreationError::new("failed to parse argument as an integer", arg_idx)
                })
        })
        .transpose()?
        .unwrap_or(1);
    if step <= 0 {
        return Err(OperatorCreationError::new(
            "argument must be larger than zero",
            arg_idx,
        ));
    }
    Ok(OperatorData::Up(OpUp {
        step: NonZeroUsize::new(step as usize).unwrap(),
    }))
}

pub fn create_op_up(step: usize) -> OperatorData {
    OperatorData::Up(OpUp {
        step: NonZeroUsize::new(step).expect("argument for operator `up` must be larger than zero"),
    })
}
