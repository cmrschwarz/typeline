use std::path::PathBuf;

use smallvec::SmallVec;

use crate::chain::ChainId;

#[derive(Clone, Copy)]
pub enum TextEncoding {
    ASCII,
    UTF8,
    UTF16,
    UTF32,
}

#[derive(Clone)]
pub enum DocumentSource {
    File(PathBuf),
    String(String),
    Bytes(Vec<u8>),
    Integer(i64),
    Stdin,
}

#[derive(Clone)]
pub enum DocumentReferencePoint {
    Url(String),
    Folder(PathBuf),
}

pub type DocumentId = usize;

#[derive(Clone)]
pub struct Document {
    pub source: DocumentSource,
    pub reference_point: Option<DocumentReferencePoint>,
    pub target_chains: SmallVec<[ChainId; 2]>,
}
