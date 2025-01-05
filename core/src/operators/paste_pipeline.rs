use std::sync::Arc;

use crate::{
    cli::call_expr::CallExpr,
    options::session_setup::SessionSetupData,
    record_data::{
        action_buffer::ActorId,
        field_data::FieldData,
        iter::field_iter::FieldIter,
        iter_hall::{IterKind, IterState},
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    typeline_error::TypelineError,
};

use super::{
    errors::OperatorCreationError,
    operator::{Operator, OperatorName},
    utils::{
        basic_generator::{BasicGenerator, BasicGeneratorWrapper},
        generator_transform_update::{GeneratorMode, GeneratorSequence},
    },
};

#[derive(Clone)]
pub struct OpPastePipeline {
    data: Arc<FieldData>,
}

pub struct TfPipelinePaster {
    iter_state: IterState,
    data: Arc<FieldData>,
}

impl GeneratorSequence for TfPipelinePaster {
    type Inserter<'a> = VaryingTypeInserter<&'a mut FieldData>;

    fn seq_len_total(&self) -> u64 {
        self.data.field_count() as u64
    }

    fn seq_len_rem(&self) -> u64 {
        (self.data.field_count() - self.iter_state.field_pos) as u64
    }

    fn reset_sequence(&mut self) {
        self.iter_state = IterState::default();
    }

    fn create_inserter<'a>(
        &mut self,
        field: &'a mut crate::record_data::field::Field,
    ) -> Self::Inserter<'a> {
        field.iter_hall.varying_type_inserter()
    }

    fn advance_sequence(
        &mut self,
        inserter: &mut Self::Inserter<'_>,
        count: usize,
    ) {
        let mut iter = unsafe {
            FieldIter::from_field_location(
                &*self.data,
                self.iter_state.as_field_location(),
            )
        };
        inserter.extend_from_iter(&mut iter, count, true, true);
        self.iter_state =
            iter.into_iter_state(ActorId::default(), IterKind::Undefined);
    }
}

impl BasicGenerator for OpPastePipeline {
    fn default_name(&self) -> OperatorName {
        "paste_pipeline".into()
    }

    type Gen = TfPipelinePaster;

    fn generator_mode(&self) -> GeneratorMode {
        GeneratorMode::Foreach
    }

    fn create_generator(&self) -> Self::Gen {
        TfPipelinePaster {
            iter_state: IterState::default(),
            data: self.data.clone(),
        }
    }
}

pub fn parse_op_last_cli_output(
    sess: &mut SessionSetupData,
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, TypelineError> {
    expr.reject_args()?;
    let Some(last_cli_output) = &sess.setup_settings.last_cli_output else {
        return Err(OperatorCreationError::new(
            "no previous CLI output to immitate",
            expr.span,
        )
        .into());
    };
    Ok(Box::new(BasicGeneratorWrapper::new(OpPastePipeline {
        data: last_cli_output.clone(),
    })))
}
