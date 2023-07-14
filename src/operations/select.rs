use bstr::{BStr, ByteSlice};

use crate::{
    options::argument::CliArgIdx,
    utils::string_store::{StringStore, StringStoreEntry, INVALID_STRING_STORE_ENTRY},
};

use super::{
    errors::{OperatorCreationError, OperatorSetupError},
    operator::OperatorData,
};

#[derive(Clone)]
pub struct OpSelect {
    key: String,
    pub key_interned: StringStoreEntry,
}

pub fn parse_op_select(
    value: Option<&BStr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let value_str = value
        .ok_or_else(|| OperatorCreationError::new("missing key argument for select", arg_idx))?
        .to_str()
        .map_err(|_| OperatorCreationError::new("key must be valid UTF-8", arg_idx))?;
    Ok(OperatorData::Select(OpSelect {
        key: value_str.to_owned(),
        key_interned: INVALID_STRING_STORE_ENTRY,
    }))
}

pub fn setup_op_select(
    string_store: &mut StringStore,
    op: &mut OpSelect,
) -> Result<(), OperatorSetupError> {
    op.key_interned = string_store.intern_moved(std::mem::replace(&mut op.key, Default::default()));
    Ok(())
}

pub fn create_op_select(key: String) -> OperatorData {
    OperatorData::Select(OpSelect {
        key: key,
        key_interned: INVALID_STRING_STORE_ENTRY,
    })
}
