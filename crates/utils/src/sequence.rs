use std::{
    arch::x86_64::{
        _mm256_add_epi64, _mm256_set1_epi64x, _mm256_setr_epi64x,
        _mm256_storeu_si256,
    },
    fmt::Write,
    mem::MaybeUninit,
};

use arrayvec::ArrayVec;

use typeline_core::{
    cli::call_expr::{CallExpr, Span},
    context::SessionData,
    liveness_analysis::{LivenessData, VarLivenessSlotKind},
    operators::{
        compute::operations::avx2::AVX2_I64_ELEM_COUNT,
        errors::OperatorCreationError,
        operator::{Operator, OperatorId, OperatorName},
        utils::{
            basic_generator::{BasicGenerator, BasicGeneratorWrapper},
            generator_transform_update::{GeneratorMode, GeneratorSequence},
        },
    },
    options::session_setup::SessionSetupData,
    record_data::{
        field::Field, variable_sized_type_inserter::VariableSizeTypeInserter,
    },
    utils::{
        indexing_type::IndexingType,
        int_string_conversions::{
            i64_to_str, parse_int_with_units, I64_MAX_DECIMAL_DIGITS,
        },
    },
};

const FAST_SEQ_MAX_STEP: i64 = 200;

#[derive(Clone, Copy)]
pub struct SequenceSpec {
    pub start: i64,
    pub end: i64,
    pub step: i64,
}

#[derive(Clone)]
pub struct OpSequence {
    ss: SequenceSpec,
    mode: SequenceMode,
    non_string_reads: bool,
}

#[derive(Clone, Copy)]
pub enum SequenceMode {
    Sequence,
    Enum,
    EnumUnbounded,
}

pub struct SequenceGenerator {
    pub non_string_reads: bool,
    pub ss: SequenceSpec,
    pub current_value: i64,
    pub seq_len_total: u64,
}

impl SequenceMode {
    pub fn to_str(&self) -> &'static str {
        match self {
            SequenceMode::Sequence => "seq",
            SequenceMode::Enum => "enum",
            SequenceMode::EnumUnbounded => "enum-u",
        }
    }
    pub fn to_str_with_seq_spec(&self, ss: SequenceSpec) -> String {
        let mut res = format!("{}={},{}", self.to_str(), ss.start, ss.end,);
        if ss.step != 1 {
            res.write_fmt(format_args!(",{}", ss.step)).unwrap();
        }
        res
    }
}

impl SequenceSpec {
    pub fn normalize_end(&mut self) {
        let range = self.end - self.start;
        let offset = range % self.step;
        if offset == 0 {
            return;
        }
        self.end += self.step - offset;
    }
    pub fn len(&self) -> u64 {
        ((self.end - self.start) / self.step) as u64
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn increment_int_str(data: &mut ArrayVec<u8, I64_MAX_DECIMAL_DIGITS>) {
    let mut i = data.len() - 1;
    loop {
        if data[i] < b'9' {
            data[i] += 1;
            return;
        }
        data[i] = b'0';
        if i == 0 {
            break;
        }
        i -= 1;
    }
    data.insert(0, b'1');
}

const AVX2_MIN_ELEM_COUNT: usize = 8;
unsafe fn generate_seq_avx2(
    start: i64,
    step: i64,
    iterations: usize,
    out: &mut [MaybeUninit<i64>],
) {
    unsafe {
        let stride = _mm256_set1_epi64x(4 * step);
        let mut arr = _mm256_setr_epi64x(
            start,
            start + step,
            start + 2 * step,
            start + 3 * step,
        );

        _mm256_storeu_si256(out.as_mut_ptr().cast(), arr);
        for i in 1..iterations {
            arr = _mm256_add_epi64(arr, stride);
            _mm256_storeu_si256(
                out.as_mut_ptr().add(i * AVX2_I64_ELEM_COUNT).cast(),
                arr,
            );
        }
    }
}

impl GeneratorSequence for SequenceGenerator {
    type Inserter<'a> = &'a mut Field;
    fn seq_len_total(&self) -> u64 {
        self.seq_len_total
    }

    fn seq_len_rem(&self) -> u64 {
        ((self.ss.end - self.current_value) / self.ss.step) as u64
    }

    fn reset_sequence(&mut self) {
        self.current_value = self.ss.start
    }

    fn create_inserter<'a>(
        &mut self,
        field: &'a mut Field,
    ) -> Self::Inserter<'a> {
        field
    }

    fn advance_sequence(
        &mut self,
        output_field: &mut Self::Inserter<'_>,
        mut count: usize,
    ) {
        let iter_hall = &mut output_field.iter_hall;
        if self.non_string_reads {
            let mut inserter = iter_hall.fixed_size_type_inserter::<i64>();
            if count >= AVX2_MIN_ELEM_COUNT {
                let iterations = count / AVX2_I64_ELEM_COUNT;
                let avx_count = iterations * AVX2_I64_ELEM_COUNT;
                unsafe {
                    let res = inserter.claim_uninit(avx_count);
                    generate_seq_avx2(
                        self.current_value,
                        self.ss.step,
                        iterations,
                        res,
                    );
                    self.current_value += avx_count as i64 * self.ss.step;
                }
                count -= avx_count;
            }
            inserter.reserve(count);
            for _ in 0..count {
                inserter.push(self.current_value);
                self.current_value += self.ss.step;
            }
        } else {
            let mut inserter = iter_hall.inline_str_inserter();
            if self.current_value >= 0
                && self.ss.step > 0
                && self.ss.step < FAST_SEQ_MAX_STEP
            {
                let mut int_str = ArrayVec::new();
                int_str.extend(
                    i64_to_str(false, self.current_value)
                        .as_bytes()
                        .iter()
                        .copied(),
                );
                inserter.reserve(count, int_str.len());
                for _ in 0..count {
                    inserter.push_may_rereserve(unsafe {
                        std::str::from_utf8_unchecked(&int_str)
                    });
                    for _ in 0..self.ss.step {
                        increment_int_str(&mut int_str);
                    }
                    self.current_value += self.ss.step;
                }
            } else if count > 0 {
                let mut int_str = i64_to_str(false, self.current_value);
                self.current_value += self.ss.step;
                // TODO: this whole variable sized type inserter thing sucks
                // reimplement that with a heuristic resrevation size maybe
                // in this special case we could do a perfect reserve and then
                // avoid checking all together
                inserter.drop_and_rereserve(count, int_str.len());
                inserter.push_may_rereserve(&int_str);
                for _ in 1..count {
                    int_str = i64_to_str(false, self.current_value);
                    inserter.push_may_rereserve(&int_str);
                    self.current_value += self.ss.step;
                }
            }
        }
    }
}

impl BasicGenerator for OpSequence {
    type Gen = SequenceGenerator;

    fn default_name(&self) -> OperatorName {
        match self.mode {
            SequenceMode::Sequence => "seq",
            SequenceMode::Enum | SequenceMode::EnumUnbounded => "enum",
        }
        .into()
    }

    fn generator_mode(&self) -> GeneratorMode {
        match self.mode {
            SequenceMode::Sequence => GeneratorMode::Foreach,
            SequenceMode::Enum => GeneratorMode::Alongside,
            SequenceMode::EnumUnbounded => GeneratorMode::AlongsideUnbounded,
        }
    }

    fn create_generator(&self) -> Self::Gen {
        SequenceGenerator {
            non_string_reads: self.non_string_reads,
            ss: self.ss,
            current_value: self.ss.start,
            seq_len_total: self.ss.len(),
        }
    }

    fn on_liveness_computed(
        &mut self,
        sess: &mut SessionData,
        ld: &LivenessData,
        op_id: OperatorId,
    ) {
        let output_id = sess.operator_bases[op_id].outputs_start;
        self.non_string_reads = ld
            .op_outputs_data
            .get_slot(VarLivenessSlotKind::NonStringReads)
            [output_id.into_usize()];
    }
}

pub fn parse_op_seq(
    sess: &mut SessionSetupData,
    call: &CallExpr,
    mode: SequenceMode,
    natural_number_mode: bool,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    if matches!(mode, SequenceMode::Enum | SequenceMode::EnumUnbounded)
        && call.args.is_empty()
    {
        return create_op_sequence_with_opts(
            i64::from(natural_number_mode),
            i64::MAX,
            1,
            mode,
            call.span,
        );
    }
    let value_str = call.require_single_string_arg_autoconvert(sess)?;
    let parts: ArrayVec<&str, 4> = value_str.split(',').take(4).collect();
    if parts.len() == 4 {
        return Err(OperatorCreationError::new(
            "failed to parse sequence parameter, got more than 3 comma separated values",
            call.span,
        ));
    }
    let start = match parts.len() {
        1 => i64::from(natural_number_mode),
        2 | 3 => parse_int_with_units(parts[0]).map_err(|msg| {
            OperatorCreationError::new_s(
                format!("failed to parse sequence start as an integer: {msg}"),
                call.span,
            )
        })?,
        _ => unreachable!(),
    };
    let step = parts
        .get(2)
        .map(|step| {
            parse_int_with_units(step).map_err(|msg| {
                OperatorCreationError::new_s(
                    format!(
                        "failed to parse sequence step size as an integer: {msg}"
                    ),
                    call.span,
                )
            })
        })
        .transpose()?
        .unwrap_or(1);

    let end_str = parts[match parts.len() {
        1 => 0,
        2 | 3 => 1,
        _ => unreachable!(),
    }];

    let mut end = parse_int_with_units(end_str).map_err(|msg| {
        OperatorCreationError::new_s(
            format!("failed to parse sequence end as an integer: {msg}"),
            call.span,
        )
    })?;
    if natural_number_mode {
        end += 1;
    }
    create_op_sequence_with_opts(start, end, step, mode, call.span)
}

fn create_op_sequence_with_opts(
    start: i64,
    mut end: i64,
    step: i64,
    mode: SequenceMode,
    span: Span,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    if step == 0 {
        return Err(OperatorCreationError::new(
            "sequence step size cannot be zero",
            span,
        ));
    }
    if step > 0 {
        if end < start {
            return Err(OperatorCreationError::new(
                "end of sequence with positive step size must be at least as large as it's start",
                span,
            ));
        }
        end += (end - start) % step;
    }
    if step < 0 {
        if end > start {
            return Err(OperatorCreationError::new(
                "end of sequence with negative step size must not be larger than it's start",
                span,
            ));
        }
        let rem = (start - end) % (-step);
        if rem > 0 {
            end -= -step - rem;
        }
    }
    let mut ss = SequenceSpec { start, end, step };
    ss.normalize_end();
    Ok(BasicGeneratorWrapper::new_operator(OpSequence {
        ss,
        mode,
        non_string_reads: true,
    }))
}

pub fn create_op_sequence(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    create_op_sequence_with_opts(
        start,
        end,
        step,
        SequenceMode::Sequence,
        Span::Generated,
    )
}
pub fn create_op_seq(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    create_op_sequence(start, end, step)
}
pub fn create_op_seqn(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    create_op_sequence(start, end + 1, step)
}
pub fn create_op_enum(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    create_op_sequence_with_opts(
        start,
        end,
        step,
        SequenceMode::Enum,
        Span::Generated,
    )
}
pub fn create_op_enum_unbounded(
    start: i64,
    end: i64,
    step: i64,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    create_op_sequence_with_opts(
        start,
        end,
        step,
        SequenceMode::EnumUnbounded,
        Span::Generated,
    )
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::SequenceSpec;

    #[rstest]
    #[case(1, 3, 2, 3)]
    #[case(1, 4, 2, 5)]
    #[case(-1, -4, -2, -5)]
    fn normalize_end(
        #[case] start: i64,
        #[case] end: i64,
        #[case] step: i64,
        #[case] normalized_end: i64,
    ) {
        let mut ss = SequenceSpec { start, end, step };
        ss.normalize_end();
        assert_eq!(ss.end, normalized_end);
    }
}
