#[cfg(windows)]
pub const NEWLINE: &str = "\r\n";
#[cfg(windows)]
pub const NEWLINE_BYTES: &[u8] = b"\r\0\n\0";
#[cfg(not(windows))]
pub const NEWLINE: &str = "\n";
#[cfg(not(windows))]
pub const NEWLINE_BYTES: &[u8] = b"\n";
