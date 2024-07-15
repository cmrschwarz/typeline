use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{
    cli::call_expr::Argument, operators::operator::OperatorData,
    options::session_setup::SessionSetupData,
    record_data::field_value::FieldValue, scr_error::ScrError,
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
            fn parse_call_expr(
                &self,
                _sess_opts: &mut SessionSetupData,
                _arg: &mut Argument,
            ) -> Result<Option<OperatorData>, ScrError> {
                Ok(None)
            }

            fn name(&self) -> Cow<'static, str> {
                "dummy_extension".into()
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
    fn name(&self) -> Cow<'static, str>;
    fn setup(&mut self, _registry: &mut ExtensionRegistry) {}
    fn parse_call_expr(
        &self,
        sess_opts: &mut SessionSetupData,
        arg: &mut Argument,
    ) -> Result<Option<OperatorData>, ScrError>;
}

pub fn build_empty_extension_registry() -> Arc<ExtensionRegistry> {
    Arc::new(ExtensionRegistry::default())
}
