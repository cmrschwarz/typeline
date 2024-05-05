use bstr::ByteSlice;

use crate::{
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::OperatorData,
};

#[derive(Clone)]
pub struct OpKey {
    key: String,
    pub key_interned: Option<StringStoreEntry>,
}

pub fn parse_op_key(
    value: Option<&[u8]>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| {
            OperatorCreationError::new("missing value for key", arg_idx)
        })?
        .to_str()
        .map_err(|_| {
            OperatorCreationError::new("key must be valid UTF-8", arg_idx)
        })?;
    Ok(OperatorData::Key(OpKey {
        key: value_str.to_owned(),
        key_interned: None,
    }))
}

pub fn setup_op_key(
    op: &mut OpKey,
    string_store: &mut StringStore,
) -> Result<(), OperatorSetupError> {
    op.key_interned =
        Some(string_store.intern_moved(std::mem::take(&mut op.key)));
    Ok(())
}

pub fn create_op_key(key: String) -> OperatorData {
    OperatorData::Key(OpKey {
        key,
        key_interned: None,
    })
}
