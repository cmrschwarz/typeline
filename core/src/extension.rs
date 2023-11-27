use crate::{
    cli::ParsedCliArgument,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
    utils::small_box::SmallBox,
};

pub struct ExtensionRegistry {
    pub extensions: Vec<SmallBox<dyn Extension, 8>>,
}

pub trait Extension: Send + Sync {
    fn try_match_cli_argument(
        &self,
        ctx_opts: &SessionOptions,
        arg: &ParsedCliArgument,
        args: &[Vec<u8>],
    ) -> Result<Option<OperatorData>, OperatorCreationError>;
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        #[allow(unused_mut)]
        let mut res = Self {
            extensions: Vec::new(),
        };

        #[cfg(feature = "sqlite")]
        res.extensions
            .push(SmallBox::new(ext_sqlite::SqliteExtension::new()));

        res
    }
}
