use crate::{
    cli::parse_args_as_single_str,
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
    params: &[&[u8]],
    arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let key = parse_args_as_single_str("key", params, arg_idx)?;
    Ok(OperatorData::Key(OpKey {
        key: key.to_owned(),
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
