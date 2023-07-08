use bstr::{BStr, ByteSlice};

use crate::{
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry, INVALID_STRING_STORE_ENTRY},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::OperatorData,
};

pub struct OpKey {
    key: String,
    pub key_sse: StringStoreEntry,
}

pub fn parse_op_key(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing value for key", arg_idx))?
        .to_str()
        .map_err(|_| OperatorCreationError::new("key must be valid UTF-8", arg_idx))?;
    Ok(OperatorData::Key(OpKey {
        key: value_str.to_owned(),
        key_sse: INVALID_STRING_STORE_ENTRY,
    }))
}

pub fn setup_op_key(
    string_store: &mut StringStore,
    op: &mut OpKey,
) -> Result<(), OperatorSetupError> {
    op.key_sse = string_store.intern_moved(std::mem::replace(&mut op.key, Default::default()));
    Ok(())
}
