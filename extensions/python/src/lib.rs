use py::parse_op_py;
use scr_core::{
    cli::{
        parse_args_as_single_str, reject_operator_argument, OperatorCallExpr,
    },
    extension::Extension,
    operators::{errors::OperatorCreationError, operator::OperatorData},
    options::session_options::SessionOptions,
};

extern crate scr_core;

pub mod py;

#[derive(Default)]
pub struct PythonExtension {}

impl Extension for PythonExtension {
    fn try_match_cli_argument(
        &self,
        _ctx_opts: &SessionOptions,
        arg: &OperatorCallExpr,
        _args: &[Vec<u8>],
        _next_arg_idx: &mut usize,
    ) -> Result<Option<OperatorData>, OperatorCreationError> {
        let cli_arg_idx = Some(arg.cli_arg.idx);
        if arg.op_name == "py" {
            let val =
                parse_args_as_single_str(arg.op_name, arg.value, cli_arg_idx)?;
            return parse_op_py(val.to_owned(), cli_arg_idx).map(Some);
        }
        if arg.op_name == "to_int" {
            reject_operator_argument("to_int", arg.value, cli_arg_idx)?;
            return parse_op_py("int(_)".to_string(), cli_arg_idx).map(Some);
        }
        Ok(None)
    }
}
