use crate::record_data::{
    action_buffer::ActorId,
    field_data::FieldData,
    iter::field_iter::FieldIter,
    iter_hall::{IterKind, IterState},
    push_interface::PushInterface,
    varying_type_inserter::VaryingTypeInserter,
};

use super::{
    field_value_sink::{FieldValueDataStorage, FieldValueSinkHandle},
    operator::OperatorName,
    utils::{
        basic_generator::BasicGenerator,
        generator_transform_update::{GeneratorMode, GeneratorSequence},
    },
};

#[derive(Clone)]
pub struct OpPastePipeline {
    data: FieldValueSinkHandle,
}

pub struct TfPipelinePaster {
    seq_len: usize,
    iter_state: IterState,
    data: FieldValueSinkHandle,
}

impl GeneratorSequence for TfPipelinePaster {
    type Inserter<'a> = VaryingTypeInserter<&'a mut FieldData>;

    fn seq_len_total(&self) -> u64 {
        self.seq_len as u64
    }

    fn seq_len_rem(&self) -> u64 {
        (self.seq_len - self.iter_state.field_pos) as u64
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
        // TODO: this is inefficient. maybe we can find a way to improve this?
        let data = self.data.get();
        match &*data {
            FieldValueDataStorage::Rle(field_data) => {
                let mut iter = unsafe {
                    FieldIter::from_field_location(
                        field_data,
                        self.iter_state.as_field_location(),
                    )
                };
                inserter.extend_from_iter(&mut iter, count, true, true);
                self.iter_state = iter
                    .into_iter_state(ActorId::default(), IterKind::Undefined);
            }
            FieldValueDataStorage::Flat(vec) => {
                inserter.extend_from_field_values_upacked(
                    vec[self.iter_state.field_pos..self.iter_state.field_pos]
                        .iter()
                        .take(count)
                        .cloned(),
                    true,
                    true,
                );
                self.iter_state.field_pos += count;
            }
        }
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
            seq_len: self.data.get().field_count(),
            iter_state: IterState::default(),
            data: self.data.clone(),
        }
    }
}
