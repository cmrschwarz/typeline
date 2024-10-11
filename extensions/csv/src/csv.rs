use std::{
    cell::RefMut,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, Write},
    path::PathBuf,
};

use scr_core::{
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    record_data::{
        action_buffer::ActorId, field::FieldId, field_data::FieldData,
        push_interface::PushInterface,
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    utils::{temp_vec::TransmutableContainer, test_utils::read_until_2},
};

pub struct OpCsv {
    header: bool,
    // TODO: add form that takes this from input
    input_file: PathBuf,
}

pub enum Input {
    NotStarted,
    Running(BufReader<File>),
    Error(OperatorApplicationError),
}

pub struct TfCsv<'a> {
    op: &'a OpCsv,
    input: Input,
    output_fields: Vec<FieldId>,
    inserters: Vec<VaryingTypeInserter<RefMut<'static, FieldData>>>,
    lines_produced: usize,
    actor: ActorId,
}

impl Operator for OpCsv {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "csv".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        true
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.input_accessed = false;
        access_flags.non_stringified_input_access = false;
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        TransformInstatiation::Simple(TransformData::Custom(smallbox!(
            TfCsv {
                op: self,
                inserters: Default::default(),
                output_fields: Default::default(),
                input: Input::NotStarted,
                lines_produced: 0,
                actor: job.job_data.add_actor_for_tf_state(tf_state)
            }
        )))
    }
}

fn distribute_errors(
    inserters: &mut [VaryingTypeInserter<RefMut<'_, FieldData>>],
    operator_application_error: OperatorApplicationError,
) {
    for i in inserters {
        i.push_error(operator_application_error.clone(), 1, true, true);
    }
}

impl<'a> Transform<'a> for TfCsv<'a> {
    fn display_name(&self) -> DefaultTransformName {
        "csv".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let target_batch_size = tf.desired_batch_size;
        let op_id = tf.op_id.unwrap();

        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            self.output_fields.iter().copied(),
        );

        let mut inserters = self.inserters.borrow_container();

        for &f in &self.output_fields {
            inserters.push(jd.field_mgr.get_varying_type_inserter(f));
        }

        let (reader, header_processed);
        match &mut self.input {
            Input::NotStarted => match File::open(&self.op.input_file) {
                Ok(file) => {
                    self.input = Input::Running(BufReader::new(file));
                    let Input::Running(file) = &mut self.input else {
                        unreachable!()
                    };
                    reader = file;
                    header_processed = !self.op.header;
                }
                Err(e) => {
                    let err = OperatorApplicationError::new_s(
                        format!(
                            "failed to open file `{}`: {}",
                            self.op.input_file.to_string_lossy(),
                            e
                        ),
                        op_id,
                    );
                    distribute_errors(&mut inserters, err.clone());
                    jd.tf_mgr
                        .submit_batch_ready_for_more(tf_id, batch_size, ps);
                    self.input = Input::Error(err);
                    return;
                }
            },
            Input::Running(buf_reader) => {
                reader = buf_reader;
                header_processed = true;
            }
            Input::Error(operator_application_error) => {
                distribute_errors(
                    &mut inserters,
                    operator_application_error.clone(),
                );
                jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
                return;
            }
        };

        if !header_processed {
            // TODO: process header
        }

        let mut lines_produced = 0;
        let mut col_idx = 0;

        match read_in_lines(
            reader,
            &mut inserters,
            &mut lines_produced,
            &mut col_idx,
            target_batch_size,
        ) {
            Ok(done) => {
                jd.tf_mgr.unclaim_batch_size(tf_id, batch_size);
                jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
                reader.seek(std::io::SeekFrom::Start(0));
            }
            Err(io_error) => {
                let err = OperatorApplicationError::new_s(
                    format!(
                        "{}:{}:{} {}",
                        self.op.input_file.to_string_lossy(),
                        lines_produced,
                        col_idx,
                        io_error
                    ),
                    op_id,
                );
                for (idx, ins) in inserters.iter_mut().enumerate() {
                    let count =
                        if col_idx == 0 || col_idx < idx { 1 } else { 2 };
                    ins.push_error(err.clone(), count, true, true);
                }
            }
        }
    }
}

fn add_inserter(
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'_, FieldData>>>,
) {
    // TODO: add additional inserter and fill him up with nones
}

fn read_in_lines(
    reader: &mut BufReader<File>,
    inserters: &mut Vec<VaryingTypeInserter<RefMut<'_, FieldData>>>,
    lines_produced: &mut usize,
    col_idx: &mut usize,
    lines_max: usize,
) -> Result<bool, std::io::Error> {
    let max_line = *lines_produced + lines_max;
    loop {
        let mut c = 0;
        if reader.read(std::slice::from_mut(&mut c))? != 1 {
            return Ok(true);
        }
        if c == b'\r' {
            if reader.read(std::slice::from_mut(&mut c))? != 1 {
                return Ok(true);
            }
            if c != b'\n' {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "expected \\n after \\r",
                ));
            }
        }
        if c == b'\n' {
            let remaining_inserters = &mut inserters[*col_idx..];
            if let Some(i) = remaining_inserters.first_mut() {
                i.push_str("", 1, true, true);
            }
            for i in remaining_inserters.iter_mut().skip(1) {
                i.push_null(1, true);
            }
            *col_idx = 0;
            *lines_produced += 1;
            if *lines_produced == max_line {
                return Ok(reader.fill_buf()?.is_empty());
            }
            continue;
        }
        let inserter = &mut inserters[*col_idx];
        let newline;
        if c == b'"' {
            // todo: parse quoted text
            newline = false;
        } else {
            let mut stream = inserter.bytes_insertion_stream(1);
            let _ = stream.write_all(std::slice::from_mut(&mut c));
            if read_until_2(reader, &mut stream, b',', b'\n')? == 0 {
                return Ok(true);
            }
            let buf = stream.get_inserted_data();
            let l = buf.len();
            newline = buf[l - 1] == b'\n';
            if newline && l > 1 && buf[l - 2] == b'\r' {
                stream.truncate(l - 2);
            } else {
                stream.truncate(l - 1);
            }
        }
        if !newline {
            *col_idx += 1;
            if *col_idx >= inserters.len() {
                add_inserter(inserters);
            }
            continue;
        }
        for i in &mut inserters[*col_idx + 1..] {
            i.push_null(1, true);
        }
        *col_idx = 0;
        *lines_produced += 1;
        if *lines_produced == max_line {
            return Ok(reader.fill_buf()?.is_empty());
        }
    }
}

pub fn create_op_csv(
    input_file: impl Into<PathBuf>,
    header: bool,
) -> OperatorData {
    OperatorData::Custom(smallbox!(OpCsv {
        input_file: input_file.into(),
        header
    }))
}
