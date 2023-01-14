use smallvec::SmallVec;

#[derive(Clone, Copy)]
pub enum DataKind {
    Html,
    Text,
    Bytes,
    Png,
}

pub struct HtmlMatchData {}

pub enum MatchData {
    Html(HtmlMatchData),
    Text(String),
    Bytes(Vec<u8>),
    Png(Vec<u8>),
}

impl MatchData {
    pub fn kind(&self) -> DataKind {
        match &self {
            MatchData::Html(_) => DataKind::Html,
            MatchData::Text(_) => DataKind::Text,
            MatchData::Bytes(_) => DataKind::Bytes,
            MatchData::Png(_) => DataKind::Png,
        }
    }
}

pub enum StreamChunk<'a> {
    Bytes(&'a [u8]),
    Text(&'a str),
}

pub struct TfBase {
    pub data_kind: DataKind,
    pub is_stream: bool,
    pub requires_eval: bool,
    pub dependants: SmallVec<[Box<dyn Transform>; 1]>,
    pub stack_index: u32,
}

pub trait Transform: Send + Sync {
    fn base(&self) -> &TfBase;
    fn base_mut(&mut self) -> &mut TfBase;
    fn process_chunk<'a: 'b, 'b>(
        &'a mut self,
        tf_stack: &'b [&'a dyn Transform],
        sc: &'a StreamChunk,
    ) -> Option<&'a StreamChunk>;
    fn evaluate<'a, 'b>(&'a mut self, tf_stack: &'b [&'a dyn Transform]);
    fn data<'a, 'b>(&'a self, tf_stack: &'b [&'a dyn Transform]) -> Option<&'a MatchData>;
}
