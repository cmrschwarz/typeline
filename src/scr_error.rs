use thiserror::Error;

use crate::{
    cli::CliArgumentError,
    operations::{
        transform::TransformApplicationError, OperationApplicationError, OperationCreationError,
        OperationSetupError,
    },
};

#[derive(Error, Debug)]
pub enum ScrError {
    #[error(transparent)]
    CliArgumentError(#[from] CliArgumentError),
    #[error(transparent)]
    OperationCreationError(#[from] OperationCreationError),
    #[error(transparent)]
    OperationSetupError(#[from] OperationSetupError),
    #[error(transparent)]
    OperationApplicationError(#[from] OperationApplicationError),
    #[error(transparent)]
    TransformApplicationError(#[from] TransformApplicationError),
}
