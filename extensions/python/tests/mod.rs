use scr_core::{
    options::context_builder::ContextBuilder, scr_error::ScrError,
};
use scr_ext_python::py::create_op_py;

#[test]
fn python_basic() -> Result<(), ScrError> {
    let res = ContextBuilder::default()
        .add_op(create_op_py("print(free_var, free_var_2)").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["print('asdf')"]);
    Ok(())
}
