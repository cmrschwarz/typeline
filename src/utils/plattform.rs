#[cfg(windows)]
pub const NEWLINE: &'static str = "\r\n";
#[cfg(windows)]
pub const NEWLINE_BYTES: &'static [u8] = b"\r\0\n\0";
#[cfg(not(windows))]
pub const NEWLINE: &'static str = "\n";
#[cfg(not(windows))]
pub const NEWLINE_BYTES: &'static [u8] = b"\n";
