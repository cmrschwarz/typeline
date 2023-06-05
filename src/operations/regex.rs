use bstring::bstr;
use regex::Regex;

use crate::options::argument::CliArgIdx;

use super::operator_base::OperatorCreationError;

pub struct OpRegex {
    pub regex: Regex,
}

pub struct TfRegex<'a> {
    pub regex: &'a Regex,
}

pub fn parse_regex_op(
    value: Option<&bstr>,
    arg_idx: Option<CliArgIdx>,
) -> Result<OpRegex, OperatorCreationError> {
    if let Some(value) = value {
        match value.to_str() {
            //TODO: we should support byte regex
            Err(_) => Err(OperatorCreationError::new(
                "regex pattern must be legal UTF-8",
                arg_idx,
            )),
            Ok(value) => Ok(OpRegex {
                regex: Regex::new(value)
                    .map_err(|_| OperatorCreationError::new("failed to compile regex", arg_idx))?,
            }),
        }
    } else {
        return Err(OperatorCreationError::new(
            "regex needs an argument",
            arg_idx,
        ));
    }
}

/*
fn process(
    &mut self,
    _ctx: &SessionData,
    _args: &HashMap<String, SmallVec<[(TransformStackIndex, MatchData); 1]>>,
    tfo: &TransformOutput,
    output: &mut VecDeque<TransformOutput>,
) -> Result<(), TransformApplicationError> {
    if tfo.is_last_chunk.is_some() {
        return Err(TransformApplicationError::new(
            "the regex transform does not support streams",
            self.op_ref,
        ));
    }
    match &tfo.data {
        Some(MatchData::Text(text)) => {
            let mut match_index = tfo.match_index;
            for cap in self.regex.captures_iter(text) {
                output.push_back(TransformOutput {
                    match_index,
                    data: Some(MatchData::Text(cap.get(0).unwrap().as_str().to_owned())),
                    args: Vec::default(), //todo
                    is_last_chunk: None,
                });
                match_index += 1;
            }
            Ok(())
        }
        Some(md) => Err(TransformApplicationError {
            message: format!(
                "the regex transform does not support match data kind '{}'",
                md.kind().to_str()
            ),
            op_ref: self.op_ref,
        }),
        None => Err(TransformApplicationError::new(
            "unexpected none match for regex transform",
            self.op_ref,
        )),
    }
}
*/
