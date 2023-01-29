use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
};

use smallvec::SmallVec;

use crate::{
    context::ContextData,
    operations::transform::{DataKind, MatchData, TfBase, Transform},
};

use super::{
    transform::{TransformApplicationError, TransformOutput, TransformStackIndex},
    OpBase, Operation, OperationApplicationError, OperationRef,
};

#[derive(Clone)]
pub struct StringSinkHandle {
    string: Arc<Mutex<String>>,
}

impl StringSinkHandle {
    pub fn new() -> StringSinkHandle {
        StringSinkHandle {
            string: Arc::new(Mutex::new(String::new())),
        }
    }
    pub fn get(&self) -> MutexGuard<String> {
        self.string.lock().unwrap()
    }
    pub fn get_string_copy(&mut self) -> String {
        self.string.lock().unwrap().clone()
    }
}

#[derive(Clone)]
pub struct OpStringSink {
    pub op_base: OpBase,
    ssh: StringSinkHandle,
}

impl OpStringSink {
    pub fn new(ssh: StringSinkHandle) -> OpStringSink {
        OpStringSink {
            op_base: OpBase::new("string_sink".to_owned(), None, None, None),
            ssh,
        }
    }
}

impl Operation for OpStringSink {
    fn base(&self) -> &super::OpBase {
        &self.op_base
    }

    fn base_mut(&mut self) -> &mut super::OpBase {
        &mut self.op_base
    }

    fn apply(
        &self,
        op_ref: OperationRef,
        tf_stack: &mut [Box<dyn Transform>],
    ) -> Result<Box<dyn Transform>, OperationApplicationError> {
        let (parent, tf_stack) = tf_stack.split_last_mut().unwrap();
        let mut tf_base = TfBase::from_parent(parent);
        tf_base.needs_stdout = true;
        tf_base.data_kind = DataKind::None;
        let tfp = Box::new(TfStringSink {
            tf_base,
            op_ref,
            ssh: self.ssh.clone(),
        });
        parent
            .add_dependant(tf_stack, tfp.base().tfs_index)
            .map_err(|tae| {
                OperationApplicationError::from_transform_application_error(
                    tae,
                    self.op_base.op_id.unwrap(),
                )
            })?;
        Ok(tfp)
    }
}

struct TfStringSink {
    tf_base: TfBase,
    op_ref: OperationRef,
    ssh: StringSinkHandle,
}

impl Transform for TfStringSink {
    fn base(&self) -> &TfBase {
        &self.tf_base
    }

    fn base_mut(&mut self) -> &mut TfBase {
        &mut self.tf_base
    }

    fn process(
        &mut self,
        _ctx: &ContextData,
        _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
        tfo: &TransformOutput,
        _output: &mut VecDeque<TransformOutput>,
    ) -> Result<(), TransformApplicationError> {
        match &tfo.data {
            Some(MatchData::Bytes(_)) => {
                return Err(TransformApplicationError::new(
                    "the string_sink transform does not support raw bytes",
                    self.op_ref,
                ));
            }
            Some(MatchData::Text(t)) => self.ssh.get().push_str(t),
            Some(MatchData::Html(_)) => {
                return Err(TransformApplicationError::new(
                    "the print transform does not support html",
                    self.op_ref,
                ));
            }
            Some(MatchData::Png(_)) => {
                return Err(TransformApplicationError::new(
                    "the print transform does not support images",
                    self.op_ref,
                ));
            }
            _ => panic!("missing TfSerialize"),
        }
        Ok(())
    }
}
