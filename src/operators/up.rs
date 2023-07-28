use crate::{
    chain::{Chain, ChainId},
    options::argument::CliArgIdx,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorData, OperatorId},
};

#[derive(Clone)]
pub struct OpUp {
    pub start_chain: ChainId,
    pub step: u32,
    pub err_level: Option<u32>,
    pub subchain_count_after: u32,
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
                .parse::<u32>()
                .map_err(|e| {
                    OperatorCreationError::new_s(
                        format!("failed to parse argument as an integer: {e}"),
                        arg_idx,
                    )
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
        step,
        // filled by add_op
        subchain_count_after: 0,
        err_level: None,
        start_chain: 0,
    }))
}

pub fn create_op_up(step: u32) -> OperatorData {
    OperatorData::Up(OpUp {
        step,
        // filled by add_op
        subchain_count_after: 0,
        err_level: None,
        start_chain: 0,
    })
}

pub fn setup_op_up(
    _chain: &Chain,
    op: &mut OpUp,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if let Some(err_level) = op.err_level {
        if err_level == 0 {
            return Err(OperatorSetupError::new(
                "cannot go up from the root chain",
                op_id,
            ));
        }
        return Err(OperatorSetupError::new_s(
            format!(
                "cannot go up {} times, chain only has <{}> parents",
                op.step, err_level
            ),
            op_id,
        ));
    }
    Ok(())
}
