use std::{any::Any, borrow::Cow, cmp::Ordering, fmt::Debug};

use thiserror::Error;

use crate::{
    operators::{errors::OperatorApplicationError, operator::OperatorId},
    utils::{
        counting_writer::{
            LengthAndCharsCountingWriter, LengthCountingWriter, TextInfo,
            TextInfoWriter,
        },
        text_write::{MaybeTextWrite, TextWrite, TextWriteIoAdapter},
    },
};

use super::formattable::RealizedFormatKey;

pub fn custom_data_reference_eq<T: CustomData + ?Sized>(
    lhs: &T,
    rhs: &dyn CustomData,
) -> bool {
    if std::mem::size_of_val(lhs) == 0 {
        return std::mem::size_of_val(rhs) == 0
            && lhs.type_id() == rhs.type_id();
    }
    let self_ptr = std::ptr::from_ref(lhs).cast::<u8>();
    let other_ptr = std::ptr::from_ref(rhs).cast::<u8>();
    self_ptr == other_ptr
}

#[derive(Error, Debug)]
pub enum FieldValueFormattingError {
    #[error("not supported")]
    NotSupported,
    #[error("IO Error: {0}")]
    IoError(std::io::Error),
    #[error("{0}")]
    Other(Cow<'static, str>),
}

#[derive(Debug)]
pub enum CustomDataWritingError {}

impl From<std::io::Error> for FieldValueFormattingError {
    fn from(e: std::io::Error) -> Self {
        FieldValueFormattingError::IoError(e)
    }
}

impl From<Cow<'static, str>> for FieldValueFormattingError {
    fn from(e: Cow<'static, str>) -> Self {
        FieldValueFormattingError::Other(e)
    }
}

impl FieldValueFormattingError {
    pub fn into_io_error(self) -> std::io::Error {
        match self {
            FieldValueFormattingError::NotSupported => {
                std::io::ErrorKind::Unsupported.into()
            }
            FieldValueFormattingError::IoError(e) => e,
            FieldValueFormattingError::Other(e) => std::io::Error::other(e),
        }
    }
    pub fn build_message_string(&self, custom_data_name: &str) -> String {
        match self {
            FieldValueFormattingError::NotSupported =>
                format!("formatting custom type '{custom_data_name}' is not supported"),
            FieldValueFormattingError::IoError(e) =>
                format!("IO error during formatting of custom type '{custom_data_name}': {e}"),
            FieldValueFormattingError::Other(e) =>
                format!("failed to format custom type '{custom_data_name}': {e}" ),
        }
    }
    pub fn as_operator_application_error(
        &self,
        op_id: OperatorId,
        custom_data_name: &str,
    ) -> OperatorApplicationError {
        OperatorApplicationError::new_s(
            self.build_message_string(custom_data_name),
            op_id,
        )
    }
}

pub trait CustomData: Any + Send + Sync + Debug {
    fn clone_dyn(&self) -> CustomDataBox;
    fn type_name(&self) -> Cow<str>;
    fn cmp(&self, _rhs: &dyn CustomData) -> Option<Ordering> {
        None
    }

    fn format(
        &self,
        w: &mut dyn TextWrite,
        format: &RealizedFormatKey,
    ) -> Result<(), FieldValueFormattingError>;
    // should return the number of characters that a subsequent
    // `format` / `format_expect_len` will write.
    fn formatted_len(
        &self,
        format: &RealizedFormatKey,
    ) -> Result<usize, FieldValueFormattingError> {
        let mut w = TextWriteIoAdapter(LengthCountingWriter::default());
        self.format(&mut w, format)?;
        Ok(w.len)
    }
    fn formatted_text_info(
        &self,
        format: &RealizedFormatKey,
    ) -> Result<TextInfo, FieldValueFormattingError> {
        let mut w =
            TextWriteIoAdapter(LengthAndCharsCountingWriter::default());
        self.format(&mut w, format)?;
        Ok(TextInfo {
            len: w.len,
            char_count: w.char_count,
            valid_utf8: true,
        })
    }

    fn format_raw(
        &self,
        w: &mut dyn MaybeTextWrite,
        format: &RealizedFormatKey,
    ) -> Result<(), FieldValueFormattingError> {
        self.format(w.as_text_write(), format)?;
        Ok(())
    }
    // should return the number of characters that a subsequent
    // `format_raw` / `format_raw_expect_len` will write.
    fn formatted_len_raw(
        &self,
        format: &RealizedFormatKey,
    ) -> Result<usize, FieldValueFormattingError> {
        let mut w = TextWriteIoAdapter(LengthCountingWriter::default());
        self.format_raw(&mut w, format)?;
        Ok(w.len)
    }
    fn formatted_text_info_raw(
        &self,
        format: &RealizedFormatKey,
    ) -> Result<TextInfo, FieldValueFormattingError> {
        let mut w = TextInfoWriter::default();
        self.format_raw(&mut *w, format)?;
        Ok(w.into_text_info())
    }
}

pub type CustomDataBox = Box<dyn CustomData>;

impl Clone for CustomDataBox {
    fn clone(&self) -> Self {
        self.clone_dyn()
    }
}
impl PartialEq for CustomDataBox {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(&**other) == Some(Ordering::Equal)
    }
}
impl PartialOrd for CustomDataBox {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cmp(&**other)
    }
}

pub fn format_custom_data_padded<C: CustomData, W: TextWrite + ?Sized>(
    _cd: &C,
    _allow_truncation: bool,
    _format: &RealizedFormatKey,
    w: &mut W,
    format_unpadded: impl FnOnce(&mut W) -> Result<(), FieldValueFormattingError>,
) -> Result<(), FieldValueFormattingError> {
    // TODO: make CustomData a Formattable, integrate with calc_fmt_layout ...
    format_unpadded(w)
}

pub fn format_custom_data_padded_raw<
    C: CustomData,
    W: MaybeTextWrite + ?Sized,
>(
    _cd: &C,
    _allow_truncation: bool,
    _format: &RealizedFormatKey,
    w: &mut W,
    format_unpadded: impl FnOnce(&mut W) -> Result<(), FieldValueFormattingError>,
) -> Result<(), FieldValueFormattingError> {
    // TODO: make CustomData a Formattable, integrate with calc_fmt_layout ...
    format_unpadded(w)
}
