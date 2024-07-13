use crate::{
    chain::BufferingMode,
    cli::try_parse_bool,
    record_data::{
        field_value::FieldValue,
        scope_manager::{Atom, ScopeId, ScopeManager},
    },
    typelist,
    utils::string_store::{StringStore, StringStoreEntry},
};
use bstr::ByteSlice;

use num::Zero;
use std::{
    ffi::OsString,
    marker::PhantomData,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::PathBuf,
    sync::Arc,
};

pub trait SettingTypeConverter<T> {
    fn convert_to_type(
        value: &FieldValue,
    ) -> Result<T, SettingConversionError>;
    fn convert_from_type(
        value: T,
    ) -> Result<FieldValue, SettingConversionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingConversionError {
    pub message: String,
}

pub type ChainSettingNames = [StringStoreEntry; chain_settings_list::COUNT];

pub trait ChainSetting: chain_settings_list::TypeList {
    const NAME: &'static str;
    const DEFAULT: Self::Type;
    type Type;
    type Converter: SettingTypeConverter<Self::Type>;

    fn lookup_ref<R>(
        sm: &ScopeManager,
        names: &ChainSettingNames,
        scope_id: ScopeId,
        mut accessor: impl FnMut(&FieldValue) -> R,
    ) -> Option<R> {
        sm.lookup_value_cell(scope_id, names[Self::INDEX], |v| {
            v.atom
                .as_ref()
                .map(|v| accessor(&mut v.value.read().unwrap()))
        })
    }

    fn lookup_ref_mut<R>(
        sm: &ScopeManager,
        names: &ChainSettingNames,
        scope_id: ScopeId,
        mut accessor: impl FnMut(&mut FieldValue) -> R,
    ) -> Option<R> {
        sm.lookup_value_cell(scope_id, names[Self::INDEX], |v| {
            v.atom
                .as_ref()
                .map(|v| accessor(&mut v.value.write().unwrap()))
        })
    }

    fn lookup(
        sm: &ScopeManager,
        names: &ChainSettingNames,
        scope_id: ScopeId,
    ) -> Result<Self::Type, SettingConversionError> {
        Self::lookup_ref(sm, names, scope_id, |v| {
            Self::Converter::convert_to_type(v)
        })
        .unwrap_or(Ok(Self::DEFAULT))
    }

    fn assign_raw(
        string_store: &mut StringStore,
        sm: &mut ScopeManager,
        scope_id: ScopeId,
        value: FieldValue,
    ) {
        match &mut sm
            .insert_value_cell(
                scope_id,
                string_store.intern_static(Self::NAME),
            )
            .atom
        {
            Some(v) => *v.value.write().unwrap() = value,
            cell @ None => *cell = Some(Arc::new(Atom::new(value))),
        }
    }
    fn assign(
        string_store: &mut StringStore,
        sm: &mut ScopeManager,
        scope_id: ScopeId,
        value: Self::Type,
    ) -> Result<(), SettingConversionError> {
        Self::assign_raw(
            string_store,
            sm,
            scope_id,
            Self::Converter::convert_from_type(value)?,
        );
        Ok(())
    }
}

impl SettingConversionError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

pub struct SettingConverterUsize<
    S: ChainSetting,
    const ALLOW_ZERO: bool = true,
>(PhantomData<S>);

impl<S: ChainSetting, const ALLOW_ZERO: bool> SettingTypeConverter<usize>
    for SettingConverterUsize<S, ALLOW_ZERO>
{
    fn convert_to_type(
        v: &FieldValue,
    ) -> Result<usize, SettingConversionError> {
        let &FieldValue::Int(value) = v else {
            return Err(SettingConversionError::new(format!(
                "value for setting %{} must be an integer",
                S::NAME
            )));
        };
        if value == 0 && !ALLOW_ZERO {
            return Err(SettingConversionError::new(format!(
                "value for setting %{} cannot be zero",
                S::NAME
            )));
        }
        match usize::try_from(value) {
            Ok(v) => Ok(v),
            Err(_) => Err(SettingConversionError::new(format!(
                "value for setting %{} must be a positive integer below 2^{}",
                S::NAME,
                usize::BITS
            ))),
        }
    }

    fn convert_from_type(
        value: usize,
    ) -> Result<FieldValue, SettingConversionError> {
        if value == 0 && !ALLOW_ZERO {
            return Err(SettingConversionError::new(format!(
                "value for setting %{} cannot be zero",
                S::NAME,
            )));
        }
        match i64::try_from(value) {
            Ok(v) => Ok(FieldValue::Int(v)),
            Err(_) => Err(SettingConversionError::new(format!(
                "value for setting %{} must be below 2^{}",
                S::NAME,
                usize::BITS
            ))),
        }
    }
}

pub struct SettingConverterBool<S: ChainSetting>(PhantomData<S>);
impl<S: ChainSetting> SettingTypeConverter<bool> for SettingConverterBool<S> {
    fn convert_to_type(
        value: &FieldValue,
    ) -> Result<bool, SettingConversionError> {
        match value {
            FieldValue::Undefined | FieldValue::Null => Ok(false),
            FieldValue::Int(v) => Ok(*v == 0),
            FieldValue::BigInt(v) => Ok(v.is_zero()),
            FieldValue::Float(v) => Ok(*v == 0.0),
            FieldValue::BigRational(v) => Ok(v.is_zero()),
            FieldValue::Text(_) | FieldValue::Bytes(_) => {
                let value = value.as_maybe_text_ref().unwrap();
                if let Some(v) = try_parse_bool(value.as_bytes()) {
                    return Ok(v);
                };
                Err(SettingConversionError::new(format!(
                    "setting %{} expects a boolean, got '{}'",
                    S::NAME,
                    value.as_bytes().to_str_lossy()
                )))
            }
            FieldValue::Argument(v) => Self::convert_to_type(&v.value),
            FieldValue::Array(_)
            | FieldValue::Object(_)
            | FieldValue::Custom(_)
            | FieldValue::Error(_)
            | FieldValue::StreamValueId(_)
            | FieldValue::FieldReference(_)
            | FieldValue::SlicedFieldReference(_) => {
                Err(SettingConversionError::new(format!(
                    "setting %{} expects a boolean, got type {}",
                    S::NAME,
                    value.kind().to_str()
                )))
            }
        }
    }

    fn convert_from_type(
        value: bool,
    ) -> Result<FieldValue, SettingConversionError> {
        Ok(FieldValue::Text(
            if value { "true" } else { "false" }.to_string(),
        ))
    }
}

pub struct SettingConverterBufferingMode<S: ChainSetting>(PhantomData<S>);
impl<S: ChainSetting> SettingTypeConverter<BufferingMode>
    for SettingConverterBufferingMode<S>
{
    fn convert_to_type(
        value: &FieldValue,
    ) -> Result<BufferingMode, SettingConversionError> {
        let FieldValue::Text(value) = value else {
            return Err(SettingConversionError::new(format!(
                "value for setting %{} must be a valid line buffering condition, got type `{}`",
                S::NAME,
                value.kind().to_str()
            )));
        };
        match &**value {
        "never" => Ok(BufferingMode::BlockBuffer),
        "always" => Ok(BufferingMode::LineBuffer),
        "stdin" => Ok(BufferingMode::LineBufferStdin),
        "tty" => Ok(BufferingMode::LineBufferIfTTY),
        "stdin-if-tty" => Ok(BufferingMode::LineBufferStdinIfTTY),
        other => Err(SettingConversionError::new(format!(
            "invalid value for setting %{}, not a valid line buffering condition: '{}'",
            S::NAME,
            other,
        ))),
    }
    }

    fn convert_from_type(
        value: BufferingMode,
    ) -> Result<FieldValue, SettingConversionError> {
        let v = match value {
            BufferingMode::BlockBuffer => "never",
            BufferingMode::LineBuffer => "always",
            BufferingMode::LineBufferStdin => "stdin",
            BufferingMode::LineBufferIfTTY => "tty",
            BufferingMode::LineBufferStdinIfTTY => "stdin-if-tty",
        };
        Ok(FieldValue::Text(v.to_string()))
    }
}

pub struct SettingConverterOptionalPath<S: ChainSetting>(PhantomData<S>);
impl<S: ChainSetting> SettingTypeConverter<Option<PathBuf>>
    for SettingConverterOptionalPath<S>
{
    fn convert_to_type(
        value: &FieldValue,
    ) -> Result<Option<PathBuf>, SettingConversionError> {
        match value {
            FieldValue::Undefined | FieldValue::Null => Ok(None),
            FieldValue::Text(v) => Ok(Some(PathBuf::from(v))),
            FieldValue::Bytes(v) => {
                Ok(Some(PathBuf::from(OsString::from_vec(v.clone()))))
            }
            FieldValue::Int(_)
            | FieldValue::BigInt(_)
            | FieldValue::Float(_)
            | FieldValue::BigRational(_)
            | FieldValue::Array(_)
            | FieldValue::Object(_)
            | FieldValue::Custom(_)
            | FieldValue::Error(_)
            | FieldValue::Argument(_)
            | FieldValue::StreamValueId(_)
            | FieldValue::FieldReference(_)
            | FieldValue::SlicedFieldReference(_) => {
                Err(SettingConversionError::new(format!(
                    "invalid value for setting %{}, expected string, got type `{}`",
                    S::NAME,
                    value.kind().to_str()
                )))
            }
        }
    }

    fn convert_from_type(
        value: Option<PathBuf>,
    ) -> Result<FieldValue, SettingConversionError> {
        match value {
            Some(v) => match v.into_os_string().into_string() {
                Ok(v) => Ok(FieldValue::Text(v)),
                // TODO: once we switch to msrv 1.74, use into_encoded_bytes
                // instead
                Err(v) => {
                    Ok(FieldValue::Bytes(v.as_os_str().as_bytes().to_owned()))
                }
            },
            None => Ok(FieldValue::Null),
        }
    }
}

pub struct SettingBatchSize;
impl ChainSetting for SettingBatchSize {
    type Type = usize;
    const NAME: &'static str = "bs";
    const DEFAULT: usize = 1024;
    type Converter = SettingConverterUsize<Self, false>;
}

pub struct SettingStreamSizeThreshold;
impl ChainSetting for SettingStreamSizeThreshold {
    type Type = usize;
    const NAME: &'static str = "sst";
    const DEFAULT: usize = 1024;
    type Converter = SettingConverterUsize<Self, false>;
}

pub struct SettingStreamBufferSize;
impl ChainSetting for SettingStreamBufferSize {
    type Type = usize;
    const NAME: &'static str = "sbs";
    const DEFAULT: usize = 1024;
    type Converter = SettingConverterUsize<Self, false>;
}

pub struct SettingPrintRationalsRaw;
impl ChainSetting for SettingPrintRationalsRaw {
    type Type = bool;
    const NAME: &'static str = "prr";
    const DEFAULT: bool = false;
    type Converter = SettingConverterBool<Self>;
}

pub struct SettingUseFloatingPointMath;
impl ChainSetting for SettingUseFloatingPointMath {
    type Type = bool;
    const NAME: &'static str = "fpm";
    const DEFAULT: bool = false;
    type Converter = SettingConverterBool<Self>;
}

pub struct SettingBufferingMode;
impl ChainSetting for SettingBufferingMode {
    type Type = BufferingMode;
    const NAME: &'static str = "lb";
    const DEFAULT: BufferingMode = BufferingMode::LineBufferIfTTY;
    type Converter = SettingConverterBufferingMode<Self>;
}

pub struct SettingDebugLog;
impl ChainSetting for SettingDebugLog {
    type Type = Option<PathBuf>;
    const NAME: &'static str = "debug_log";
    const DEFAULT: Option<PathBuf> = None;
    type Converter = SettingConverterOptionalPath<Self>;
}

typelist! {
    pub mod chain_settings_list: (ChainSetting) = [
        SettingBatchSize,
        SettingStreamSizeThreshold,
        SettingStreamBufferSize,
        SettingPrintRationalsRaw,
        SettingUseFloatingPointMath,
        SettingBufferingMode,
        SettingDebugLog
    ]{}
}
