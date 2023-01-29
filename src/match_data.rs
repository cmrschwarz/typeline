use std::fmt::Display;

#[derive(Clone, Copy)]
pub enum MatchDataKind {
    Html,
    Text,
    Bytes,
    Png,
    None,
}

impl MatchDataKind {
    pub fn to_str(&self) -> &'static str {
        match self {
            MatchDataKind::Html => "html",
            MatchDataKind::Text => "text",
            MatchDataKind::Bytes => "bytes",
            MatchDataKind::Png => "png",
            MatchDataKind::None => "none",
        }
    }
}

impl Display for MatchDataKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

#[derive(Clone)]
pub struct HtmlMatchData {}

impl Display for HtmlMatchData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "todo: serialize html!")
    }
}

#[derive(Clone)]
pub enum MatchData {
    Html(HtmlMatchData),
    Text(String),
    Bytes(Vec<u8>),
    Png(Vec<u8>),
}

impl MatchData {
    pub fn kind(&self) -> MatchDataKind {
        match &self {
            MatchData::Html(_) => MatchDataKind::Html,
            MatchData::Text(_) => MatchDataKind::Text,
            MatchData::Bytes(_) => MatchDataKind::Bytes,
            MatchData::Png(_) => MatchDataKind::Png,
        }
    }
}
