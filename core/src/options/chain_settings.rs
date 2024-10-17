use crate::{
    cli::{
        call_expr::{Argument, Span},
        try_parse_bool,
    },
    record_data::{
        field_value::FieldValue,
        field_value_ref::FieldValueRef,
        scope_manager::{Atom, ScopeId, ScopeManager, ScopeValue},
    },
    typelist,
    utils::string_store::StringStoreEntry,
};

use bstr::ByteSlice;
use num::Zero;
use std::{
    ffi::OsString, marker::PhantomData, os::unix::ffi::OsStringExt,
    path::PathBuf, sync::Arc,
};
use thiserror::Error;

pub trait SettingTypeConverter {
    type Type;
    fn convert_to_type(
        value: FieldValueRef,
    ) -> Result<Self::Type, SettingConversionError>;
    fn convert_from_type(
        value: Self::Type,
    ) -> Result<FieldValue, SettingConversionError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct SettingConversionError {
    pub message: String,
}

#[derive(Clone, Copy, Default)]
pub enum BufferingMode {
    BlockBuffer,
    LineBuffer,
    LineBufferStdin,
    #[default]
    LineBufferIfTTY,
    LineBufferStdinIfTTY,
}

pub const RARTIONAL_DECIMALS_DEFAULT_CUTOFF: u32 = 40;

#[derive(Clone, Copy)]
pub enum RationalsPrintMode {
    Cutoff(u32),
    Raw,
    Dynamic,
    // attempts to use plain if losslessly possible, otherwise uses raw
}

pub type ChainSettingNames = [StringStoreEntry; chain_settings_list::COUNT];

pub trait ChainSetting: chain_settings_list::TypeList {
    const NAME: &'static str;
    const DEFAULT: Self::Type;
    type Type;
    type Converter: SettingTypeConverter<Type = Self::Type>;

    fn lookup(
        sm: &ScopeManager,
        names: &ChainSettingNames,
        scope_id: ScopeId,
    ) -> Option<(Result<Self::Type, SettingConversionError>, Span)> {
        sm.visit_value(scope_id, names[Self::INDEX], |v| {
            let atom = v.atom()?;
            let value = atom.value.read().unwrap();
            if let FieldValue::Argument(arg) = &*value {
                Some((
                    Self::Converter::convert_to_type(arg.value.as_ref()),
                    arg.span,
                ))
            } else {
                Some((
                    Self::Converter::convert_to_type(value.as_ref()),
                    Span::Generated,
                ))
            }
        })
    }

    fn assign_raw(
        sm: &mut ScopeManager,
        names: &ChainSettingNames,
        scope_id: ScopeId,
        value: FieldValue,
    ) {
        match sm.scopes[scope_id].values.entry(names[Self::INDEX]) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                match e.get_mut() {
                    ScopeValue::Atom(v) => {
                        *v.value.write().unwrap() = value;
                    }
                    other => {
                        *other = ScopeValue::Atom(Arc::new(Atom::new(value)));
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(ScopeValue::Atom(Arc::new(Atom::new(value))));
            }
        }
    }

    fn assign(
        sm: &mut ScopeManager,
        chain_names: &ChainSettingNames,
        scope_id: ScopeId,
        value: Self::Type,
        span: Span,
    ) -> Result<(), SettingConversionError> {
        let value = FieldValue::Argument(Box::new(Argument {
            value: Self::Converter::convert_from_type(value)?,
            span,
            source_scope: scope_id,
            meta_info: None,
        }));
        Self::assign_raw(sm, chain_names, scope_id, value);
        Ok(())
    }
}

impl SettingConversionError {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl Default for RationalsPrintMode {
    fn default() -> Self {
        RationalsPrintMode::Cutoff(RARTIONAL_DECIMALS_DEFAULT_CUTOFF)
    }
}

pub struct SettingConverterUsize<
    S: ChainSetting,
    const ALLOW_ZERO: bool = true,
>(PhantomData<S>);

impl<S: ChainSetting, const ALLOW_ZERO: bool> SettingTypeConverter
    for SettingConverterUsize<S, ALLOW_ZERO>
{
    type Type = usize;
    fn convert_to_type(
        v: FieldValueRef,
    ) -> Result<usize, SettingConversionError> {
        let FieldValueRef::Int(&value) = v else {
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
impl<S: ChainSetting> SettingTypeConverter for SettingConverterBool<S> {
    type Type = bool;
    fn convert_to_type(
        value: FieldValueRef,
    ) -> Result<bool, SettingConversionError> {
        match value {
            FieldValueRef::Undefined | FieldValueRef::Null => Ok(false),
            FieldValueRef::Int(v) => Ok(*v == 0),
            FieldValueRef::BigInt(v) => Ok(v.is_zero()),
            FieldValueRef::Float(v) => Ok(*v == 0.0),
            FieldValueRef::BigRational(v) => Ok(v.is_zero()),
            FieldValueRef::Text(_) | FieldValueRef::Bytes(_) => {
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
            FieldValueRef::Argument(v) => {
                Self::convert_to_type(v.value.as_ref())
            }
            FieldValueRef::Array(_)
            | FieldValueRef::Object(_)
            | FieldValueRef::Custom(_)
            | FieldValueRef::Error(_)
            | FieldValueRef::Macro(_)
            | FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                Err(SettingConversionError::new(format!(
                    "setting %{} expects a boolean, got type {}",
                    S::NAME,
                    value.repr().kind().to_str()
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
impl<S: ChainSetting> SettingTypeConverter
    for SettingConverterBufferingMode<S>
{
    type Type = BufferingMode;
    fn convert_to_type(
        value: FieldValueRef,
    ) -> Result<BufferingMode, SettingConversionError> {
        let FieldValueRef::Text(value) = value else {
            return Err(SettingConversionError::new(format!(
                "invalid line buffering condition for %{}, got type `{}`",
                S::NAME,
                value.kind().to_str()
            )));
        };
        match value {
            "never" => Ok(BufferingMode::BlockBuffer),
            "always" => Ok(BufferingMode::LineBuffer),
            "stdin" => Ok(BufferingMode::LineBufferStdin),
            "tty" => Ok(BufferingMode::LineBufferIfTTY),
            "stdin-if-tty" => Ok(BufferingMode::LineBufferStdinIfTTY),
            other => Err(SettingConversionError::new(format!(
                "invalid line buffering condition for %{}, got '{}'",
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

pub struct SettingConverterRationalsPrintMode<S: ChainSetting>(PhantomData<S>);
impl<S: ChainSetting> SettingTypeConverter
    for SettingConverterRationalsPrintMode<S>
{
    type Type = RationalsPrintMode;
    fn convert_to_type(
        value: FieldValueRef,
    ) -> Result<RationalsPrintMode, SettingConversionError> {
        let FieldValueRef::Text(value) = value else {
            return Err(SettingConversionError::new(format!(
                "invalid rational printing variant for %{}, got type `{}`",
                S::NAME,
                value.kind().to_str()
            )));
        };
        match value {
            "raw" => return Ok(RationalsPrintMode::Raw),
            "dynamic" => return Ok(RationalsPrintMode::Dynamic),
            "cutoff" => {
                return Ok(RationalsPrintMode::Cutoff(
                    RARTIONAL_DECIMALS_DEFAULT_CUTOFF,
                ))
            }
            _ => (),
        }
        if let Some(amount) = value.strip_prefix("cutoff-") {
            if let Ok(n) = amount.parse::<u32>() {
                return Ok(RationalsPrintMode::Cutoff(n));
            }
            return Err(SettingConversionError::new(format!(
                "cutoff amount for %{} must be a valid integer, got '{}'",
                S::NAME,
                amount,
            )));
        }
        Err(SettingConversionError::new(format!(
            "invalid rational printing variant for %{}, got '{}'",
            S::NAME,
            value,
        )))
    }

    fn convert_from_type(
        value: RationalsPrintMode,
    ) -> Result<FieldValue, SettingConversionError> {
        let v = match value {
            RationalsPrintMode::Cutoff(v) => {
                if v == RARTIONAL_DECIMALS_DEFAULT_CUTOFF {
                    "cutoff".to_string()
                } else {
                    format!("cutoff-{v}")
                }
            }
            RationalsPrintMode::Raw => "raw".to_string(),
            RationalsPrintMode::Dynamic => "dynamic".to_string(),
        };
        Ok(FieldValue::Text(v))
    }
}

pub struct SettingConverterPath<S: ChainSetting>(PhantomData<S>);
impl<S: ChainSetting> SettingTypeConverter for SettingConverterPath<S> {
    type Type = PathBuf;
    fn convert_to_type(
        value: FieldValueRef,
    ) -> Result<PathBuf, SettingConversionError> {
        match value {
            FieldValueRef::Text(v) => Ok(PathBuf::from(v)),
            FieldValueRef::Bytes(v) => {
                Ok(PathBuf::from(OsString::from_vec(v.to_owned())))
            }
             FieldValueRef::Undefined | FieldValueRef::Null  |
            FieldValueRef::Int(_)
            | FieldValueRef::BigInt(_)
            | FieldValueRef::Float(_)
            | FieldValueRef::BigRational(_)
            | FieldValueRef::Array(_)
            | FieldValueRef::Object(_)
            | FieldValueRef::Custom(_)
            | FieldValueRef::Error(_)
            | FieldValueRef::Macro(_)
            | FieldValueRef::Argument(_)
            | FieldValueRef::StreamValueId(_)
            | FieldValueRef::FieldReference(_)
            | FieldValueRef::SlicedFieldReference(_) => {
                Err(SettingConversionError::new(format!(
                    "invalid value for setting %{}, expected string, got type `{}`",
                    S::NAME,
                    value.kind().to_str()
                )))
            }
        }
    }

    fn convert_from_type(
        value: PathBuf,
    ) -> Result<FieldValue, SettingConversionError> {
        match value.into_os_string().into_string() {
            Ok(v) => Ok(FieldValue::Text(v)),
            Err(v) => Ok(FieldValue::Bytes(v.into_encoded_bytes())),
        }
    }
}

pub struct SettingConverterOptional<SC: SettingTypeConverter>(PhantomData<SC>);

impl<SC: SettingTypeConverter> SettingTypeConverter
    for SettingConverterOptional<SC>
{
    type Type = Option<SC::Type>;
    fn convert_to_type(
        value: FieldValueRef,
    ) -> Result<Option<SC::Type>, SettingConversionError> {
        match value {
            FieldValueRef::Null | FieldValueRef::Undefined => Ok(None),
            _ => Ok(Some(SC::convert_to_type(value)?)),
        }
    }

    fn convert_from_type(
        value: Option<SC::Type>,
    ) -> Result<FieldValue, SettingConversionError> {
        match value {
            Some(v) => SC::convert_from_type(v),
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

pub struct SettingRationalsPrintMode;
impl ChainSetting for SettingRationalsPrintMode {
    type Type = RationalsPrintMode;
    const NAME: &'static str = "rpm";
    const DEFAULT: RationalsPrintMode =
        RationalsPrintMode::Cutoff(RARTIONAL_DECIMALS_DEFAULT_CUTOFF);
    type Converter = SettingConverterRationalsPrintMode<Self>;
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
    type Converter = SettingConverterOptional<SettingConverterPath<Self>>;
}

pub struct SettingDebugLogNoApply;
impl ChainSetting for SettingDebugLogNoApply {
    type Type = bool;
    const NAME: &'static str = "debug_log_no_apply";
    const DEFAULT: bool = false;
    type Converter = SettingConverterBool<Self>;
}

pub struct SettingDebugLogStepMin;
impl ChainSetting for SettingDebugLogStepMin {
    type Type = usize;
    const NAME: &'static str = "debug_log_step_min";
    const DEFAULT: usize = 0;
    type Converter = SettingConverterUsize<Self>;
}

pub struct SettingDebugBreakOnStep;
impl ChainSetting for SettingDebugBreakOnStep {
    type Type = Option<usize>;
    const NAME: &'static str = "debug_break_on_step";
    const DEFAULT: Option<usize> = None;
    type Converter = SettingConverterOptional<SettingConverterUsize<Self>>;
}

pub struct SettingMaxThreads;
impl ChainSetting for SettingMaxThreads {
    type Type = usize;
    const NAME: &'static str = "j";
    const DEFAULT: usize = 0;
    type Converter = SettingConverterUsize<Self, true>;
}

pub struct SettingActionListCleanupFrequency;
impl ChainSetting for SettingActionListCleanupFrequency {
    type Type = usize;
    const NAME: &'static str = "alcf";
    const DEFAULT: usize = 64;
    type Converter = SettingConverterUsize<Self, true>;
}

typelist! {
    pub mod chain_settings_list: (ChainSetting) = [
        SettingBatchSize,
        SettingStreamSizeThreshold,
        SettingStreamBufferSize,
        SettingRationalsPrintMode,
        SettingUseFloatingPointMath,
        SettingBufferingMode,
        SettingDebugLog,
        SettingDebugLogNoApply,
        SettingDebugLogStepMin,
        SettingDebugBreakOnStep,
        SettingMaxThreads,
        SettingActionListCleanupFrequency
    ]{}
}
