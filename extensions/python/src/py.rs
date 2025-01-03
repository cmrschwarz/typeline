use std::ffi::{CStr, CString};

use num::{BigInt, BigRational};
use pyo3::{
    conversion::FromPyObject,
    ffi::{PyObject, PyTypeObject},
    pybacked::{PyBackedBytes, PyBackedStr},
    types::{
        PyAnyMethods, PyBytes, PyCode, PyDict, PyList, PyListMethods,
        PyString, PyTypeMethods,
    },
    Bound, IntoPyObject, Py, PyAny, Python,
};
use typeline_core::{
    chain::ChainId,
    cli::call_expr::Span,
    context::SessionData,
    job::{Job, JobData},
    liveness_analysis::{
        BasicBlockId, LivenessData, OpOutputIdx, OperatorLivenessOutput,
    },
    operators::{
        errors::{OperatorApplicationError, OperatorCreationError},
        operator::{
            OffsetInChain, Operator, OperatorDataId, OperatorId,
            OperatorOffsetInChain, PreboundOutputsMap, TransformInstatiation,
        },
        transform::{Transform, TransformId, TransformState},
    },
    options::session_setup::SessionSetupData,
    record_data::{
        array::Array,
        field::{CowFieldDataRef, FieldIterRef},
        field_data_ref::DestructuredFieldDataRef,
        field_value::{FieldValue, Object, ObjectKeysStored},
        field_value_ref::FieldValueRef,
        iter::{field_iter::FieldIter, ref_iter::AutoDerefIter},
        iter_hall::IterKind,
        push_interface::PushInterface,
    },
    typeline_error::TypelineError,
    utils::{
        lazy_lock_guard::LazyRwLockGuard,
        phantom_slot::PhantomSlot,
        string_store::{StringStore, StringStoreEntry},
        temp_vec::{transmute_vec, TransmutableContainer},
    },
};

struct PyTypes {
    none_type: Py<PyTypeObject>,
    int_type: Py<PyTypeObject>,
    float_type: Py<PyTypeObject>,
    str_type: Py<PyTypeObject>,
    bytes_type: Py<PyTypeObject>,
    list_type: Py<PyTypeObject>,
    dict_type: Py<PyTypeObject>,
    rational_type: Option<Py<PyTypeObject>>,
}

pub struct OpPy {
    free_vars_str: Vec<String>,
    free_vars_py_str: Vec<Py<PyString>>,
    // populated during setup
    free_vars_sse: Vec<Option<StringStoreEntry>>,
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
                FieldIter<DestructuredFieldDataRef<'static>>,
            >,
        >,
    >,
}
unsafe impl<'a> Send for TfPy<'a> {}

impl Operator for OpPy {
    fn default_name(
        &self,
    ) -> typeline_core::operators::operator::OperatorName {
        "py".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }

    fn register_output_var_names(
        &self,
        ld: &mut LivenessData,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) {
        for &fv in &self.free_vars_sse {
            ld.add_var_name_opt(fv);
        }
    }

    fn setup(
        &mut self,
        sess: &mut SessionSetupData,
        op_data_id: OperatorDataId,
        chain_id: ChainId,
        operator_offset_in_chain: OperatorOffsetInChain,
        span: Span,
    ) -> Result<OperatorId, TypelineError> {
        for fvs in &self.free_vars_str {
            if fvs == "_" {
                self.free_vars_sse.push(None);
            } else {
                self.free_vars_sse
                    .push(Some(sess.string_store.intern_cloned(fvs)));
            }
        }
        Ok(sess.add_op(op_data_id, chain_id, operator_offset_in_chain, span))
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
        sess: &SessionData,
        ld: &mut LivenessData,
        op_offset_after_last_write: OffsetInChain,
        op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        output: &mut OperatorLivenessOutput,
    ) {
        output.flags.may_dup_or_drop = false;
        for fv in &self.free_vars_sse {
            if let Some(name) = fv {
                ld.access_var(
                    sess,
                    op_id,
                    ld.var_names[name],
                    op_offset_after_last_write,
                    true,
                );
            } else {
                output.flags.input_accessed = true;
                output.flags.non_stringified_input_access = true;
            }
        }
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
        let ms = &jd.match_set_mgr.match_sets[tf_state.match_set_id];
        let next_actor_id = ms.action_buffer.borrow().peek_next_actor_id();
        for fv in &self.free_vars_sse {
            let field_id = if let Some(name) = fv {
                if let Some(id) = jd.scope_mgr.lookup_field(
                    jd.match_set_mgr.match_sets[tf_state.match_set_id]
                        .active_scope,
                    *name,
                ) {
                    jd.field_mgr.setup_field_refs(&mut jd.match_set_mgr, id);
                    let mut f = jd.field_mgr.fields[id].borrow_mut();
                    f.ref_count += 1;
                    id
                } else {
                    jd.match_set_mgr.get_dummy_field_with_ref_count(
                        &jd.field_mgr,
                        tf_state.match_set_id,
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
                    next_actor_id,
                    IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
                ),
            });
        }

        let locals = Python::with_gil(|py| unsafe {
            let locals = pyo3::ffi::PyDict_New();
            let none = pyo3::ffi::Py_None();
            for var in &self.free_vars_py_str {
                pyo3::ffi::PyDict_SetItem(locals, var.as_ptr(), none);
            }
            Py::from_owned_ptr(py, locals)
        });

        TransformInstatiation::Single(Box::new(TfPy {
            op: self,
            input_field_refs: Vec::with_capacity(input_fields.len()),
            input_field_iters: Vec::with_capacity(input_fields.len()),
            input_fields,
            locals,
        }))
    }
}

enum PythonValue {
    InlineText(PyBackedStr),
    InlineBytes(PyBackedBytes),
    BigInt(BigInt),
    Rational(BigRational),
    Other(FieldValue),
}

impl PythonValue {
    pub fn into_field_value(self) -> FieldValue {
        match self {
            PythonValue::InlineText(v) => FieldValue::Text(v.to_string()),
            PythonValue::InlineBytes(v) => FieldValue::Bytes(v.to_vec()),
            PythonValue::BigInt(v) => FieldValue::BigInt(Box::new(v)),
            PythonValue::Rational(v) => FieldValue::BigRational(Box::new(v)),
            PythonValue::Other(v) => v,
        }
    }
}

fn python_type_name(value: Bound<PyAny>) -> String {
    value
        .get_type()
        .name()
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string())
}

fn try_extract_rational(
    _py: Python,
    py_val: &Bound<PyAny>,
) -> Option<BigRational> {
    let numerator =
        BigInt::extract_bound(&py_val.getattr("numerator").ok()?).ok()?;
    let denominator =
        BigInt::extract_bound(&py_val.getattr("denominator").ok()?).ok()?;
    Some(BigRational::new_raw(numerator, denominator))
}

fn get_python_value(
    py: Python,
    py_types: &PyTypes,
    py_val: Bound<PyAny>,
    op_id: OperatorId,
) -> PythonValue {
    let type_ptr = py_val.get_type_ptr().cast::<PyObject>();
    if type_ptr == py_types.none_type.as_ptr() {
        return PythonValue::Other(FieldValue::Null);
    }
    if type_ptr == py_types.int_type.as_ptr() {
        if let Ok(i) = i64::extract_bound(&py_val) {
            return PythonValue::Other(FieldValue::Int(i));
        }
        if let Ok(i) = BigInt::extract_bound(&py_val) {
            return PythonValue::BigInt(i);
        }
    }
    if type_ptr == py_types.float_type.as_ptr() {
        if let Ok(f) = f64::extract_bound(&py_val) {
            return PythonValue::Other(FieldValue::Float(f));
        }
    }
    if type_ptr == py_types.str_type.as_ptr() {
        if let Ok(s) = py_val.extract::<PyBackedStr>() {
            return PythonValue::InlineText(s);
        }
    }
    if type_ptr == py_types.bytes_type.as_ptr() {
        if let Ok(b) = py_val.extract::<PyBackedBytes>() {
            return PythonValue::InlineBytes(b);
        }
    }
    if type_ptr == py_types.list_type.as_ptr() {
        let list = py_val.downcast_into::<PyList>().unwrap();
        let mut arr = Array::default();
        for v in &list {
            // PERF: this will potentially box and unbox BigInts.
            arr.push(
                get_python_value(py, py_types, v, op_id).into_field_value(),
            );
        }
        return PythonValue::Other(FieldValue::Array(arr));
    }
    if type_ptr == py_types.dict_type.as_ptr() {
        let py_dict = py_val.downcast_into::<PyDict>().unwrap();
        let mut result = ObjectKeysStored::default();
        for (k, v) in &py_dict {
            let Ok(key) = String::extract_bound(&k) else {
                return PythonValue::Other(FieldValue::Error(
                    OperatorApplicationError::new_s(
                        format!(
                            "dict keys must be string, not `{}`",
                            python_type_name(k)
                        ),
                        op_id,
                    ),
                ));
            };
            let value =
                get_python_value(py, py_types, v, op_id).into_field_value();
            // PERF: this will potentially box and unbox BigInts.
            result.insert(key.to_string(), value);
        }
        return PythonValue::Other(FieldValue::Object(Box::new(
            Object::KeysStored(result),
        )));
    }
    if Some(type_ptr) == py_types.rational_type.as_ref().map(|v| v.as_ptr()) {
        if let Some(rational) = try_extract_rational(py, &py_val) {
            return PythonValue::Rational(rational);
        }
        return PythonValue::Other(FieldValue::Error(OperatorApplicationError::new_s(
            format!("failed to convert python object of type `{}` into Rational", python_type_name(py_val)),
            op_id,
        )));
    }
    PythonValue::Other(FieldValue::Error(OperatorApplicationError::new_s(
        format!(
            "unsupported python result type: {}",
            python_type_name(py_val)
        ),
        op_id,
    )))
}

fn to_python_object<'a>(
    val: FieldValueRef<'_>,
    py: Python<'a>,
    lazy_string_store: &mut LazyRwLockGuard<'_, StringStore>,
) -> Result<Option<Bound<'a, PyAny>>, OperatorApplicationError> {
    match val {
        FieldValueRef::Null => {
            Ok(Some(().into_pyobject(py).unwrap().into_any()))
        }
        // TODO: maybe error / configurable
        FieldValueRef::Undefined => Ok(None),
        FieldValueRef::Int(i) => {
            Ok(Some(i.into_pyobject(py).unwrap().into_any()))
        }
        FieldValueRef::BigInt(i) => {
            Ok(Some(i.into_pyobject(py).unwrap().into_any()))
        }
        FieldValueRef::Float(f) => {
            Ok(Some(f.into_pyobject(py).unwrap().into_any()))
        }
        FieldValueRef::BigRational(_) => todo!(), // use fractions.
        // Fraction?
        FieldValueRef::Text(s) => {
            Ok(Some(s.into_pyobject(py).unwrap().into_any()))
        }
        FieldValueRef::Bytes(b) => Ok(Some(PyBytes::new(py, b).into_any())),
        FieldValueRef::Argument(_) => todo!(),
        FieldValueRef::OpDecl(_) => todo!(),
        FieldValueRef::Array(a) => {
            let res =
                PyList::new(py, std::iter::empty::<Bound<PyDict>>()).unwrap();
            for v in a.ref_iter() {
                res.append(to_python_object(v, py, lazy_string_store)?)
                    .unwrap();
            }
            Ok(Some(res.into_any()))
        }
        FieldValueRef::Object(o) => {
            let res = PyDict::new(py);
            match o {
                Object::KeysStored(index_map) => {
                    for (k, v) in index_map {
                        res.set_item(
                            k.into_pyobject(py).unwrap(),
                            to_python_object(
                                v.as_ref(),
                                py,
                                lazy_string_store,
                            )?,
                        )
                        .unwrap();
                    }
                }
                Object::KeysInterned(index_map) => {
                    for (k, v) in index_map {
                        res.set_item(
                            lazy_string_store
                                .get_mut()
                                .lookup(*k)
                                .into_pyobject(py)
                                .unwrap(),
                            to_python_object(
                                v.as_ref(),
                                py,
                                lazy_string_store,
                            )?,
                        )
                        .unwrap();
                    }
                }
            }
            Ok(Some(res.into_any()))
        }
        FieldValueRef::Custom(_) => todo!(),
        FieldValueRef::StreamValueId(_) => todo!(),
        FieldValueRef::Error(e) => Err(e.clone()),
        FieldValueRef::FieldReference(_) => todo!(),
        FieldValueRef::SlicedFieldReference(_) => todo!(),
    }
}

impl<'a> Transform<'a> for TfPy<'a> {
    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
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

        let mut lazy_string_store =
            LazyRwLockGuard::new(&jd.session_data.string_store);

        let mut output_field = jd.field_mgr.fields[output_field].borrow_mut();
        let mut inserter = output_field.iter_hall.varying_type_inserter();

        let mut input_field_refs = self.input_field_refs.take_transmute();
        let mut input_field_iters = self.input_field_iters.take_transmute();

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
            'next_element: for _ in 0..batch_size {
                // PERF: only update when changed!
                for (i, iter) in input_field_iters.iter_mut().enumerate() {
                    let val = iter
                        .next_value(&jd.match_set_mgr, 1)
                        .map(|(val, _rl, _fr_offs)| val)
                        .unwrap_or(FieldValueRef::Undefined);

                    match to_python_object(val, py, &mut lazy_string_store) {
                        Ok(Some(value)) => unsafe {
                            pyo3::ffi::PyDict_SetItem(
                                self.locals.as_ptr(),
                                self.op.free_vars_py_str[i].as_ptr(),
                                value.as_ptr(),
                            );
                        },
                        Ok(None) => unsafe {
                            pyo3::ffi::PyDict_DelItem(
                                self.locals.as_ptr(),
                                self.op.free_vars_py_str[i].as_ptr(),
                            );
                        },
                        Err(e) => {
                            inserter.push_error(e, 1, true, true);
                            continue 'next_element;
                        }
                    };
                }
                let res = unsafe {
                    if !self.op.statements.is_none(py) {
                        let stmts_res = pyo3::ffi::PyEval_EvalCode(
                            self.op.statements.as_ptr(),
                            self.op.globals.as_ptr(),
                            self.locals.as_ptr(),
                        );
                        if let Err(e) =
                            pyo3::Bound::from_owned_ptr_or_err(py, stmts_res)
                        {
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
                    pyo3::Bound::from_owned_ptr_or_err(py, res)
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
                let value =
                    get_python_value(py, &self.op.py_types, res, op_id);
                match value {
                    PythonValue::InlineText(v) => {
                        inserter.push_str(&v, 1, true, false)
                    }
                    PythonValue::InlineBytes(v) => {
                        inserter.push_bytes(&v, 1, true, false)
                    }
                    PythonValue::BigInt(v) => {
                        inserter.push_big_int(v, 1, true, false)
                    }
                    PythonValue::Rational(v) => {
                        inserter.push_big_rational(v, 1, true, false)
                    }
                    PythonValue::Other(v) => {
                        inserter.push_field_value_unpacked(v, 1, true, false)
                    }
                }
            }
        });

        for (i, iter) in input_field_iters.drain(0..).enumerate() {
            let fr = self.input_fields[i];
            jd.field_mgr.store_iter(fr.field_id, fr.iter_id, iter);
        }
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

pub fn build_op_py(
    cmd: String,
    span: Span,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    let command = match CString::new(cmd) {
        Ok(cmd) => cmd,
        Err(e) => {
            return Err(OperatorCreationError::new_s(
                format!(
                    "command contains a NULL byte as position {}",
                    e.nul_position()
                ),
                span,
            ))
        }
    };

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| unsafe {
        let none = pyo3::ffi::Py_None();
        let builtins = pyo3::ffi::PyEval_GetBuiltins();
        unsafe fn get_builtin_type(
            py: Python,
            builtins: *mut PyObject,
            name: &str,
        ) -> Py<PyTypeObject> {
            pyo3::Py::from_borrowed_ptr(
                py,
                pyo3::ffi::PyDict_GetItemString(
                    builtins,
                    name.as_ptr().cast(),
                )
                .cast(),
            )
        }
        let fractions_class = py
            .import("fractions")
            .ok()
            .and_then(|v| v.getattr("Fraction").ok())
            .map(|v| v.downcast_into_unchecked().unbind());

        let py_types = PyTypes {
            none_type: pyo3::Py::from_borrowed_ptr(
                py,
                pyo3::ffi::PyObject_Type(none),
            ),
            int_type: get_builtin_type(py, builtins, "int\0"),
            float_type: get_builtin_type(py, builtins, "float\0"),
            str_type: get_builtin_type(py, builtins, "str\0"),
            bytes_type: get_builtin_type(py, builtins, "bytes\0"),
            list_type: get_builtin_type(py, builtins, "list\0"),
            dict_type: get_builtin_type(py, builtins, "dict\0"),
            rational_type: fractions_class,
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
                span,
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
                span,
            ));
        }

        let co_names = pyo3::ffi::PyObject_GetAttrString(
            code_object,
            "co_names\0".as_ptr().cast(),
        );

        let free_var_count = pyo3::ffi::PyObject_Length(co_names);
        let mut free_vars_str = Vec::with_capacity(free_var_count as usize);
        let mut free_vars_py_str = Vec::with_capacity(free_var_count as usize);
        for fv_i in 0..free_var_count {
            let free_var_name = pyo3::ffi::PyTuple_GetItem(co_names, fv_i);
            if pyo3::ffi::PyDict_Contains(builtins, free_var_name) > 0 {
                continue;
            }
            if pyo3::ffi::PyDict_Contains(module_dict, free_var_name) > 0 {
                continue;
            }

            let var_name = pyo3::ffi::PyUnicode_AsUTF8(free_var_name);
            free_vars_py_str.push(Py::from_owned_ptr(py, free_var_name));
            free_vars_str
                .push(CStr::from_ptr(var_name).to_str().unwrap().to_owned());
        }

        let locals = PyDict::new(py);
        locals
            .set_item("code", command.as_c_str().to_str().unwrap())
            .unwrap();
        let code = cr#"
import ast
body = ast.parse(code, mode="exec").body
if len(body) > 1:
    statements = compile(ast.Module(body[:-1], []), "<cmd_stmts>", "exec")
    expression = compile(ast.Expression(body[-1].value), "<cmd_expr>", "eval")
else:
    statements = None
    expression = compile(code, "<cmd>", "eval")
"#;
        if let Err(e) = py.run(code, None, Some(&locals)) {
            return Err(OperatorCreationError::new_s(
                format!("Python Command Compilation: {e}"),
                span,
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

        Ok(Box::new(OpPy {
            free_vars_str,
            free_vars_py_str,
            free_vars_sse: Vec::new(),
            statements,
            final_expr,
            globals: pyo3::Py::from_borrowed_ptr(py, module_dict),
            py_types,
        }) as Box<dyn Operator>)
    })
}

pub fn create_op_py(
    cmd: &str,
) -> Result<Box<dyn Operator>, OperatorCreationError> {
    build_op_py(cmd.to_owned(), Span::Generated)
}
