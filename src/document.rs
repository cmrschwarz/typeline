use std::path::PathBuf;

use bstring::BString;

use crate::{chain::ChainId, operations::transform::MatchData};

#[derive(Clone)]
pub enum DocumentSource {
    Url(BString),
    File(PathBuf),
    String(String),
    Bytes(BString),
    Stdin,
}
impl DocumentSource {
    pub fn create_match_data(&self) -> MatchData {
        match self {
            DocumentSource::Url(_url) => todo!("TfDownload"),
            DocumentSource::File(_p) => todo!("TfReadFile"),
            DocumentSource::String(s) => MatchData::Text(s.clone()),
            DocumentSource::Bytes(s) => MatchData::Bytes(s.clone().into_bytes()),
            DocumentSource::Stdin => todo!("shared stream"),
        }
    }
}

#[derive(Clone)]
pub enum DocumentReferencePoint {
    Url(String),
    Folder(String),
}

#[derive(Clone)]
pub struct Document {
    pub source: DocumentSource,
    pub reference_point: Option<DocumentReferencePoint>,
    pub target_chains: Vec<ChainId>,
}
