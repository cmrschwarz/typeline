use std::ffi::{CStr, CString};

use pyo3::Python;
use scr_core::{
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::{OperatorCreationError, OperatorSetupError},
        operator::{
            Operator, OperatorData, OperatorId, OperatorOffsetInChain,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{
            DefaultTransformName, Transform, TransformData, TransformId,
            TransformState,
        },
    },
    options::argument::CliArgIdx,
    record_data::{
        action_buffer::ActorRef,
        field::{CowFieldDataRef, FieldId},
        iter_hall::IterId,
        iters::{DestructuredFieldDataRef, Iter},
        push_interface::PushInterface,
        ref_iter::AutoDerefIter,
    },
    smallbox,
    utils::phantom_slot::PhantomSlot,
};

pub struct OpPy {
    command: CString,
}

unsafe impl Send for OpPy {}
unsafe impl Sync for OpPy {}

pub struct TfPy {
    input_fields: Vec<(FieldId, IterId)>,
    input_field_refs: Vec<PhantomSlot<CowFieldDataRef<'static>>>,
    input_field_iters: Vec<
        PhantomSlot<
            AutoDerefIter<
                'static,
                Iter<'static, DestructuredFieldDataRef<'static>>,
            >,
        >,
    >,
}

impl Operator for OpPy {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "py".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }

    fn setup(
        &mut self,
        sess: &mut SessionData,
        _chain_id: OperatorId,
        op_id: OperatorId,
    ) -> Result<(), OperatorSetupError> {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| unsafe {
            let builtins = pyo3::ffi::PyEval_GetBuiltins();
            let dunder_builtins_str =
                pyo3::intern!(py, "__builtins__").as_ptr();

            let module =
                pyo3::ffi::PyImport_AddModule("__main__\0".as_ptr().cast());
            let module_dict = pyo3::ffi::PyModule_GetDict(module);
            let mut has_builtins =
                pyo3::ffi::PyDict_Contains(module_dict, dunder_builtins_str);
            if has_builtins == 0 {
                has_builtins = pyo3::ffi::PyDict_SetItem(
                    module_dict,
                    dunder_builtins_str,
                    builtins,
                );
            }
            if has_builtins == -1 {
                pyo3::PyErr::take(py);
                return Err(OperatorSetupError::new(
                    "failed to set __builtin__ on python module",
                    op_id,
                ));
            }

            let code_object = pyo3::ffi::Py_CompileString(
                self.command.as_ptr().cast(),
                "<string>\0".as_ptr().cast(),
                pyo3::ffi::Py_file_input,
            );

            if let Some(err) = pyo3::PyErr::take(py) {
                return Err(OperatorSetupError::new_s(
                    format!(
                        "failed to set __builtin__ on python module: {err}"
                    ),
                    op_id,
                ));
            }

            let co_names = pyo3::ffi::PyObject_GetAttrString(
                code_object,
                "co_names\0".as_ptr().cast(),
            );

            let free_var_count = pyo3::ffi::PyObject_Length(co_names);

            let mut free_vars = Vec::new();
            let mut string_store = sess.string_store.write().unwrap();

            for fv_i in 0..free_var_count {
                let free_var_name = pyo3::ffi::PyTuple_GetItem(co_names, fv_i);
                if pyo3::ffi::PyDict_Contains(builtins, free_var_name) > 0 {
                    continue;
                }
                if pyo3::ffi::PyDict_Contains(module_dict, free_var_name) > 0 {
                    continue;
                }
                let var_name =
                    CStr::from_ptr(pyo3::ffi::PyUnicode_AsUTF8(free_var_name));
                match var_name.to_str() {
                    Ok(s) => free_vars.push(string_store.intern_cloned(s)),
                    Err(_) => {
                        return Err(OperatorSetupError::new_s(
                            format!(
                                "free variable name contains invalid unicode: '{}'",
                                var_name.to_string_lossy()
                            ),
                            op_id,
                        ))
                    }
                };
            }
            Ok(())
        })
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OperatorOffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.input_accessed = false;
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let actor_id = job.job_data.match_set_mgr.match_sets
            [tf_state.match_set_id]
            .action_buffer
            .borrow()
            .peek_next_actor_id();
        job.job_data.field_mgr.fields[tf_state.output_field]
            .borrow_mut()
            .first_actor = ActorRef::Unconfirmed(actor_id);
        TransformInstatiation::Simple(TransformData::Custom(smallbox!(TfPy {
            input_fields: Vec::new(),
            input_field_refs: Vec::new(),
            input_field_iters: Vec::new(),
        })))
    }
}

impl Transform for TfPy {
    fn display_name(&self) -> DefaultTransformName {
        "py".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];

        let output_field = tf.output_field;
        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            [output_field],
        );

        let mut output_field = jd.field_mgr.fields[output_field].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();
        inserter.push_undefined(batch_size, true);

        jd.tf_mgr.submit_batch_ready_for_more(tf_id, batch_size, ps);
    }
}

#[allow(unused)]
unsafe fn debug_print_py_object(
    py: pyo3::Python,
    object: *mut pyo3::ffi::PyObject,
) {
    unsafe {
        let module =
            pyo3::ffi::PyImport_AddModule("__dummy__\0".as_ptr().cast());
        assert!(pyo3::PyErr::take(py).is_none());
        let globals = pyo3::ffi::PyModule_GetDict(module);

        pyo3::ffi::PyDict_SetItemString(
            globals,
            "var_to_print__\0".as_ptr().cast(),
            object,
        );
        assert!(pyo3::PyErr::take(py).is_none());

        let code_object_2 = pyo3::ffi::Py_CompileString(
            "print(var_to_print__)\0".as_ptr().cast(),
            "<string>\0".as_ptr().cast(),
            pyo3::ffi::Py_file_input,
        );
        let code_object_2 =
            pyo3::Bound::from_owned_ptr_or_err(py, code_object_2).unwrap();

        pyo3::ffi::PyEval_EvalCode(code_object_2.as_ptr(), globals, globals);
        assert!(pyo3::PyErr::take(py).is_none());
    }
}

pub fn parse_op_py(
    cmd: &str,
    cli_arg_idx: Option<CliArgIdx>,
) -> Result<OperatorData, OperatorCreationError> {
    let command = match CString::new(cmd) {
        Ok(cmd) => cmd,
        Err(e) => {
            return Err(OperatorCreationError::new_s(
                format!(
                    "command contains a NULL byte as position {}",
                    e.nul_position()
                ),
                cli_arg_idx,
            ))
        }
    };
    Ok(OperatorData::Custom(smallbox!(OpPy { command })))
}

pub fn create_op_py(cmd: &str) -> Result<OperatorData, OperatorCreationError> {
    parse_op_py(cmd, None)
}
