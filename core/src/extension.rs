use std::collections::HashMap;

use crate::{
    cli::ParsedCliArgument,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
    record_data::field_value::FieldValue,
    tyson::TysonParseError,
};

pub trait CustomTysonType: Send + Sync {
    fn parse(&self, value: &str) -> Result<FieldValue, TysonParseError>;
}

#[derive(Default)]
pub struct ExtensionRegistry {
    pub extensions: Vec<Box<dyn Extension>>,
    pub tyson_types: HashMap<Box<str>, Box<dyn CustomTysonType>>,
}

impl ExtensionRegistry {
    pub fn setup(&mut self) {
        struct DummyExt;
        impl Extension for DummyExt {
            fn try_match_cli_argument(
                &self,
                _ctx_opts: &SessionOptions,
                _arg: &ParsedCliArgument,
                _args: &[Vec<u8>],
            ) -> Result<Option<OperatorData>, OperatorCreationError>
            {
                unimplemented!()
            }
        }
        let mut dummy_ext: Box<dyn Extension> = Box::new(DummyExt);
        for i in 0..self.extensions.len() {
            let mut ext =
                std::mem::replace(&mut self.extensions[i], dummy_ext);
            ext.setup(self);
            dummy_ext = std::mem::replace(&mut self.extensions[i], ext);
        }
    }
}

pub trait Extension: Send + Sync {
    fn setup(&mut self, _registry: &mut ExtensionRegistry) {}
    fn try_match_cli_argument(
        &self,
        ctx_opts: &SessionOptions,
        arg: &ParsedCliArgument,
        args: &[Vec<u8>],
    ) -> Result<Option<OperatorData>, OperatorCreationError>;
}
