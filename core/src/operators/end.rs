use crate::{
    chain::{ChainId, SubchainIndex},
    cli::reject_operator_params,
    options::argument::CliArgIdx,
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::{OperatorData, OperatorId},
};

use crate::utils::indexing_type::IndexingType;

#[derive(Clone)]
pub struct OpEnd {
    pub chain_id_before: ChainId,
    pub chain_id_after: ChainId,
    // number of subchains that `chain_id_after` has after the `end` operator
    pub subchain_count_after: SubchainIndex,
}

pub fn create_op_end() -> OperatorData {
    let invalid_chain_id =
        ChainId::new(<ChainId as IndexingType>::IndexBaseType::MAX);
    let invalid_subchain_id = SubchainIndex::new(
        <SubchainIndex as IndexingType>::IndexBaseType::MAX,
    );
    OperatorData::End(OpEnd {
        subchain_count_after: invalid_subchain_id,
        chain_id_before: invalid_chain_id,
        chain_id_after: invalid_chain_id,
    })
}

pub fn parse_op_end(
    params: &[&[u8]],
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    reject_operator_params("end", params, arg_idx)?;
    Ok(create_op_end())
}

pub fn setup_op_end(
    op: &OpEnd,
    op_id: OperatorId,
) -> Result<(), OperatorSetupError> {
    if op.chain_id_before == ChainId::zero() {
        return Err(OperatorSetupError::new(
            "`end` operator is outside of a subchain",
            op_id,
        ));
    }
    Ok(())
}
