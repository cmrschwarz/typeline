use std::path::PathBuf;

use bstring::BString;
use smallvec::SmallVec;
use url::Url;

use crate::chain::ChainId;

#[derive(Clone)]
pub enum DocumentSource {
    Url(Url),
    File(PathBuf),
    String(String),
    Bytes(BString),
    Integer(i64),
    Stdin,
}

#[derive(Clone)]
pub enum DocumentReferencePoint {
    Url(Url),
    Folder(PathBuf),
}

pub type DocumentId = usize;

#[derive(Clone)]
pub struct Document {
    pub source: DocumentSource,
    pub reference_point: Option<DocumentReferencePoint>,
    pub target_chains: SmallVec<[ChainId; 2]>,
}
