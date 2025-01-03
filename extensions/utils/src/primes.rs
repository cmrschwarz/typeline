use primes::{PrimeSet, Sieve};
use typeline_core::{
    cli::call_expr::CallExpr,
    operators::{
        errors::OperatorCreationError,
        operator::Operator,
        utils::{
            basic_generator::{BasicGenerator, BasicGeneratorWrapper},
            generator_transform_update::{GeneratorMode, GeneratorSequence},
        },
    },
    record_data::{
        field::Field, fixed_sized_type_inserter::FixedSizeTypeInserter,
    },
};

pub struct OpPrimes;

pub struct PrimesGenerator {
    sieve: primes::Sieve,
    count: usize,
}

impl GeneratorSequence for PrimesGenerator {
    type Inserter<'a> = FixedSizeTypeInserter<'a, i64>;
    fn seq_len_total(&self) -> u64 {
        u64::MAX
    }

    fn seq_len_rem(&self) -> u64 {
        u64::MAX
    }

    fn reset_sequence(&mut self) {
        self.count = 0;
    }

    fn create_inserter<'a>(
        &mut self,
        field: &'a mut Field,
    ) -> Self::Inserter<'a> {
        field.iter_hall.fixed_size_type_inserter()
    }

    fn advance_sequence(
        &mut self,
        inserter: &mut Self::Inserter<'_>,
        count: usize,
    ) {
        inserter.extend(
            self.sieve
                .iter()
                .skip(self.count)
                .map(|v| v as i64)
                .take(count),
        );
        self.count += count;
    }
}

impl BasicGenerator for OpPrimes {
    type Gen = PrimesGenerator;

    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "primes".into()
    }

    fn generator_mode(&self) -> GeneratorMode {
        GeneratorMode::AlongsideUnbounded
    }

    fn create_generator(&self) -> Self::Gen {
        PrimesGenerator {
            sieve: Sieve::new(),
            count: 0,
        }
    }
}

pub fn create_op_primes() -> Box<dyn Operator> {
    BasicGeneratorWrapper::new_operator(OpPrimes)
}

pub fn parse_op_primes(
    expr: &CallExpr,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    expr.reject_args()?;
    Ok(create_op_primes())
}
