use std::path::PathBuf;

use bstring::BString;
use smallvec::SmallVec;
use url::Url;

use crate::{chain::ChainId, match_set::MatchSet};

#[derive(Clone)]
pub enum DocumentSource {
    Url(Url),
    File(PathBuf),
    String(String),
    Bytes(BString),
    Stdin,
}
impl DocumentSource {
    pub fn create_match_set(&self) -> MatchSet {
        todo!("create match set from document")
    }
}

#[derive(Clone)]
pub enum DocumentReferencePoint {
    Url(Url),
    Folder(PathBuf),
}

#[derive(Clone)]
pub struct Document {
    pub source: DocumentSource,
    pub reference_point: Option<DocumentReferencePoint>,
    pub target_chains: SmallVec<[ChainId; 2]>,
}
