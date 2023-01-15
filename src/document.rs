use bstring::BString;

use crate::{
    chain::ChainId,
    operations::start::TfStart,
    transform::{MatchData, Transform},
};

#[derive(Clone)]
pub enum DocumentSource {
    Url(BString),
    File(BString),
    String(String),
    Bytes(BString),
    Stdin,
}
impl DocumentSource {
    pub fn create_start_transform(&self) -> Box<dyn Transform> {
        match self {
            DocumentSource::Url(url) => todo!("TfDownload"),
            DocumentSource::File(url) => todo!("TfReadFile"),
            DocumentSource::String(s) => Box::new(TfStart::new(MatchData::Text(s.clone()))),
            DocumentSource::Bytes(s) => {
                Box::new(TfStart::new(MatchData::Bytes(s.clone().into_bytes())))
            }
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
