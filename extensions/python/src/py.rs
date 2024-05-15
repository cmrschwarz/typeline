use std::ffi::{CStr, CString};

use num::BigInt;
use pyo3::{
    ffi::{PyObject, PyTypeObject},
    types::{PyAnyMethods, PyCode, PyDict},
    Py, PyAny, Python,
};
use scr_core::{
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::{
            OperatorApplicationError, OperatorCreationError,
            OperatorSetupError,
        },
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
        field::{CowFieldDataRef, FieldIterRef},
        field_value_ref::FieldValueRef,
        iter_hall::IterKind,
        iters::{DestructuredFieldDataRef, Iter},
        push_interface::PushInterface,
        ref_iter::AutoDerefIter,
    },
    smallbox,
    utils::{
        phantom_slot::PhantomSlot,
        string_store::StringStoreEntry,
        temp_vec::{transmute_vec, transmute_vec_take},
    },
};

struct PyTypes {
    none_type: *mut pyo3::ffi::PyTypeObject,
    int_type: *mut pyo3::ffi::PyTypeObject,
    str_type: *mut pyo3::ffi::PyTypeObject,
    bytes_type: *mut pyo3::ffi::PyTypeObject,
}

pub struct OpPy {
    free_vars_str: Vec<CString>,
    // populated during setup
    free_vars: Vec<Option<StringStoreEntry>>,
    statements: Py<PyAny>, // PyCode or None
    final_expr: Py<PyCode>,
    globals: Py<PyDict>,
    py_types: PyTypes,
}
unsafe impl Send for OpPy {}
unsafe impl Sync for OpPy {}

pub struct TfPy<'a> {
    op: &'a OpPy,
    locals: Py<PyDict>,
    input_fields: Vec<FieldIterRef>,
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
unsafe impl<'a> Send for TfPy<'a> {}

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
        let mut string_store = sess.string_store.write().unwrap();
        for fvs in &self.free_vars_str {
            if fvs.as_c_str() == c"_" {
                self.free_vars.push(None);
            } else {
                match fvs.to_str() {
                    Ok(s) => {
                        self.free_vars
                            .push(Some(string_store.intern_cloned(s)));
                    }
                    Err(_) => {
                        return Err(OperatorSetupError::new_s(
                            format!(
                                "free variable name contains invalid unicode: '{}'",
                                fvs.to_string_lossy()
                            ),
                            op_id,
                        ))
                    }
                };
            }
        }
        Ok(())
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
        let mut input_fields = Vec::new();
        let jd = &mut job.job_data;
        for fv in &self.free_vars {
            let field_id = if let Some(name) = fv {
                if let Some(id) = jd.match_set_mgr.match_sets
                    [tf_state.match_set_id]
                    .field_name_map
                    .get(name)
                    .copied()
                {
                    jd.field_mgr.setup_field_refs(&mut jd.match_set_mgr, id);
                    let mut f = jd.field_mgr.fields[id].borrow_mut();
                    f.ref_count += 1;
                    id
                } else {
                    jd.field_mgr.add_field(
                        &mut jd.match_set_mgr,
                        tf_state.match_set_id,
                        Some(*name),
                        jd.field_mgr.get_first_actor(tf_state.input_field),
                    )
                }
            } else {
                let mut f =
                    jd.field_mgr.fields[tf_state.input_field].borrow_mut();
                // while the ref count was already bumped by the transform
                // creation cleaning up this transform is
                // simpler this way
                f.ref_count += 1;
                tf_state.input_field
            };

            input_fields.push(FieldIterRef {
                field_id,
                iter_id: jd.field_mgr.claim_iter(
                    field_id,
                    IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
                ),
            });
        }

        let locals = Python::with_gil(|py| unsafe {
            let locals = pyo3::ffi::PyDict_New();
            let none = pyo3::ffi::Py_None();
            for var in &self.free_vars_str {
                pyo3::ffi::PyDict_SetItemString(locals, var.as_ptr(), none);
            }
            Py::from_owned_ptr(py, locals)
        });

        TransformInstatiation::Simple(TransformData::Custom(smallbox!(TfPy {
            op: self,
            input_field_refs: Vec::with_capacity(input_fields.len()),
            input_field_iters: Vec::with_capacity(input_fields.len()),
            input_fields,
            locals
        })))
    }
}

impl<'a> Transform<'a> for TfPy<'a> {
    fn display_name(&self) -> DefaultTransformName {
        "py".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        use pyo3::conversion::{FromPyObject, ToPyObject};
        let (batch_size, ps) = jd.tf_mgr.claim_batch(tf_id);
        let tf = &jd.tf_mgr.transforms[tf_id];
        let op_id = tf.op_id.unwrap();

        let output_field = tf.output_field;
        jd.tf_mgr.prepare_for_output(
            &mut jd.field_mgr,
            &mut jd.match_set_mgr,
            tf_id,
            [output_field],
        );

        let mut output_field = jd.field_mgr.fields[output_field].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();

        let mut input_field_refs =
            transmute_vec_take(&mut self.input_field_refs);
        let mut input_field_iters =
            transmute_vec_take(&mut self.input_field_iters);

        for iter_ref in &self.input_fields {
            input_field_refs.push(
                jd.field_mgr
                    .get_cow_field_ref(&jd.match_set_mgr, iter_ref.field_id),
            );
        }
        for (i, iter_ref) in self.input_fields.iter().enumerate() {
            input_field_iters.push(jd.field_mgr.get_auto_deref_iter(
                iter_ref.field_id,
                &input_field_refs[i],
                iter_ref.iter_id,
            ));
        }
        Python::with_gil(|py| {
            for _ in 0..batch_size {
                for (i, iter) in input_field_iters.iter_mut().enumerate() {
                    let val = iter
                        .next_value(&jd.match_set_mgr, 1)
                        .map(|(val, _rl, _fr_offs)| val)
                        .unwrap_or(FieldValueRef::Undefined);

                    let obj = match val {
                        FieldValueRef::Null => ().to_object(py),
                        // TODO: maybe error / configurable
                        FieldValueRef::Undefined => {
                            unsafe {
                                pyo3::ffi::PyDict_DelItemString(
                                    self.locals.as_ptr(),
                                    self.op.free_vars_str[i].as_ptr(),
                                );
                            }
                            continue;
                        }
                        FieldValueRef::Int(i) => i.to_object(py),
                        FieldValueRef::BigInt(i) => i.to_object(py),
                        FieldValueRef::Float(f) => f.to_object(py),
                        FieldValueRef::Rational(_) => todo!(), /* use fractions.Fraction? */
                        FieldValueRef::Text(s) => s.to_object(py),
                        FieldValueRef::Bytes(b) => b.to_object(py),
                        FieldValueRef::Array(_) => todo!(),
                        FieldValueRef::Object(_) => todo!(),
                        FieldValueRef::Custom(_) => todo!(),
                        FieldValueRef::StreamValueId(_) => todo!(),
                        FieldValueRef::Error(_) => todo!(),
                        FieldValueRef::FieldReference(_) => todo!(),
                        FieldValueRef::SlicedFieldReference(_) => todo!(),
                    };
                    unsafe {
                        pyo3::ffi::PyDict_SetItemString(
                            self.locals.as_ptr(),
                            self.op.free_vars_str[i].as_ptr(),
                            obj.as_ptr(),
                        );
                    }
                }
                let res = unsafe {
                    if !self.op.statements.is_none(py) {
                        pyo3::ffi::PyEval_EvalCode(
                            self.op.statements.as_ptr(),
                            self.op.globals.as_ptr(),
                            self.locals.as_ptr(),
                        );
                        if let Some(e) = pyo3::PyErr::take(py) {
                            inserter.push_error(
                                OperatorApplicationError::new_s(
                                    format!("Python: {e}"),
                                    op_id,
                                ),
                                1,
                                true,
                                false,
                            );
                            continue;
                        }
                    }
                    let res = pyo3::ffi::PyEval_EvalCode(
                        self.op.final_expr.as_ptr(),
                        self.op.globals.as_ptr(),
                        self.locals.as_ptr(),
                    );
                    pyo3::Bound::from_borrowed_ptr_or_err(py, res)
                };

                let res = match res {
                    Ok(v) => v,
                    Err(e) => {
                        inserter.push_error(
                            OperatorApplicationError::new_s(
                                format!("Python: {e}"),
                                op_id,
                            ),
                            1,
                            true,
                            false,
                        );
                        continue;
                    }
                };
                let type_ptr = res.get_type_ptr();
                let pv = &self.op.py_types;
                if type_ptr == pv.none_type {
                    inserter.push_null(1, true);
                    continue;
                }
                if type_ptr == pv.int_type {
                    if let Ok(i) = i64::extract_bound(&res) {
                        inserter.push_int(i, 1, true, true);
                    }
                    if let Ok(i) = BigInt::extract_bound(&res) {
                        inserter.push_big_int(i, 1, true, true);
                    }
                    continue;
                }
                if type_ptr == pv.str_type {
                    if let Ok(s) = <&str>::extract_bound(&res) {
                        inserter.push_str(s, 1, true, true);
                    }
                    continue;
                }
                if type_ptr == pv.bytes_type {
                    if let Ok(b) = <&[u8]>::extract_bound(&res) {
                        // PERF: maybe force inline
                        inserter.push_bytes(b, 1, true, true);
                    }
                    continue;
                }
                // TODO:  Handle objects as dicts
                inserter.push_error(
                    OperatorApplicationError::new_s(
                        format!(
                            "unsupported python result type: {}",
                            unsafe {
                                CStr::from_ptr(pyo3::ffi::PyUnicode_AsUTF8(
                                    pyo3::ffi::PyObject_Str(type_ptr.cast()),
                                ))
                                .to_string_lossy()
                            }
                        ),
                        op_id,
                    ),
                    1,
                    true,
                    false,
                );
            }
        });

        self.input_field_iters = transmute_vec(input_field_iters);
        self.input_field_refs = transmute_vec(input_field_refs);

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

        let code_object = pyo3::ffi::Py_CompileString(
            "print(var_to_print__)\0".as_ptr().cast(),
            "<string>\0".as_ptr().cast(),
            pyo3::ffi::Py_file_input,
        );
        let code_object_2 =
            pyo3::Bound::from_owned_ptr_or_err(py, code_object).unwrap();

        pyo3::ffi::PyEval_EvalCode(code_object_2.as_ptr(), globals, globals);
        assert!(pyo3::PyErr::take(py).is_none());
    }
}

pub fn parse_op_py(
    cmd: String,
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

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| unsafe {
        let none = pyo3::ffi::Py_None();
        let builtins = pyo3::ffi::PyEval_GetBuiltins();
        unsafe fn get_builtin_type(
            builtins: *mut PyObject,
            name: &CStr,
        ) -> *mut PyTypeObject {
            pyo3::ffi::PyDict_GetItemString(builtins, name.as_ptr()).cast()
        }
        let py_types = PyTypes {
            none_type: pyo3::ffi::PyObject_Type(none).cast(),
            int_type: get_builtin_type(builtins, c"int"),
            str_type: get_builtin_type(builtins, c"str"),
            bytes_type: get_builtin_type(builtins, c"bytes"),
        };

        let dunder_builtins_str = pyo3::intern!(py, "__builtins__").as_ptr();

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
            return Err(OperatorCreationError::new(
                "failed to set __builtin__ on python module",
                cli_arg_idx,
            ));
        }

        let code_object = pyo3::ffi::Py_CompileString(
            command.as_ptr().cast(),
            "<cmd>\0".as_ptr().cast(),
            pyo3::ffi::Py_file_input,
        );

        if let Some(err) = pyo3::PyErr::take(py) {
            return Err(OperatorCreationError::new_s(
                format!("Python failed to parse: {err}"),
                cli_arg_idx,
            ));
        }

        let co_names = pyo3::ffi::PyObject_GetAttrString(
            code_object,
            "co_names\0".as_ptr().cast(),
        );

        let free_var_count = pyo3::ffi::PyObject_Length(co_names);
        let mut free_vars_str = Vec::with_capacity(free_var_count as usize);

        for fv_i in 0..free_var_count {
            let free_var_name = pyo3::ffi::PyTuple_GetItem(co_names, fv_i);
            if pyo3::ffi::PyDict_Contains(builtins, free_var_name) > 0 {
                continue;
            }
            if pyo3::ffi::PyDict_Contains(module_dict, free_var_name) > 0 {
                continue;
            }
            let var_name = pyo3::ffi::PyUnicode_AsUTF8(free_var_name);

            free_vars_str.push(CStr::from_ptr(var_name).to_owned());
        }

        let locals = PyDict::new_bound(py);
        locals
            .set_item("code", command.as_c_str().to_str().unwrap())
            .unwrap();
        let code = r#"
import ast
body = ast.parse(code, mode="exec").body
if len(body) > 1:
    statements = compile(ast.Module(body[:-1], []), "<cmd_stmts>", "exec")
    expression = compile(ast.Expression(body[-1].value), "<cmd_expr>", "eval")
else:
    statements = None
    expression = compile(code, "<cmd>", "eval")
"#;
        if let Err(e) = py.run_bound(code, None, Some(&locals)) {
            return Err(OperatorCreationError::new_s(
                format!("Python Command Compilation: {e}"),
                cli_arg_idx,
            ));
        }
        let statements = locals.get_item("statements").unwrap().unbind();

        let final_expr = locals
            .get_item("expression")
            .unwrap()
            .clone()
            .downcast_into::<pyo3::types::PyCode>()
            .unwrap()
            .unbind();

        Ok(OperatorData::Custom(smallbox!(OpPy {
            free_vars_str,
            free_vars: Vec::new(),
            statements,
            final_expr,
            globals: pyo3::Py::from_borrowed_ptr(py, module_dict),
            py_types
        })))
    })
}

pub fn create_op_py(cmd: &str) -> Result<OperatorData, OperatorCreationError> {
    parse_op_py(cmd.to_owned(), None)
}
