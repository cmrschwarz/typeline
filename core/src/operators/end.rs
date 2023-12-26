use crate::{
    chain::ChainId, cli::reject_operator_argument,
    options::argument::CliArgIdx,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorData, OperatorId},
};

#[derive(Clone)]
pub struct OpEnd {
    pub chain_id_before: ChainId,
    // number of subchains that `chain_id_after` has after the `end` operator
    pub chain_id_after: ChainId,
    pub subchain_count_after: u32,
}

pub fn create_op_end() -> OperatorData {
    OperatorData::End(OpEnd {
        subchain_count_after: u32::MAX,
        chain_id_before: ChainId::MAX,
        chain_id_after: ChainId::MAX,
    })
}

pub fn parse_op_end(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_argument("end", value, arg_idx)?;
    Ok(create_op_end())
}

pub fn setup_op_end(
    op: &OpEnd,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.chain_id_before == 0 {
        return Err(OperatorSetupError::new(
            "`end` operator is outside of a subchain",
            op_id,
        ));
    }
    Ok(())
}
