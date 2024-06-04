#![cfg(feature = "repl")]
use reedline::{
    Prompt, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus,
    PromptViMode,
};

use std::borrow::Cow;

/// The default prompt indicator
pub static DEFAULT_PROMPT_INDICATOR: &str = "> ";
pub static DEFAULT_VI_INSERT_PROMPT_INDICATOR: &str = ": ";
pub static DEFAULT_VI_NORMAL_PROMPT_INDICATOR: &str = ">";
pub static DEFAULT_MULTILINE_INDICATOR: &str = "::: ";

#[derive(Default)]
pub struct ScrPrompt {}

impl Prompt for ScrPrompt {
    fn render_prompt_left(&self) -> Cow<str> {
        Cow::Borrowed("scr")
    }

    fn render_prompt_right(&self) -> Cow<str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, edit_mode: PromptEditMode) -> Cow<str> {
        match edit_mode {
            PromptEditMode::Default | PromptEditMode::Emacs => {
                DEFAULT_PROMPT_INDICATOR.into()
            }
            PromptEditMode::Vi(vi_mode) => match vi_mode {
                PromptViMode::Normal => {
                    DEFAULT_VI_NORMAL_PROMPT_INDICATOR.into()
                }
                PromptViMode::Insert => {
                    DEFAULT_VI_INSERT_PROMPT_INDICATOR.into()
                }
            },
            PromptEditMode::Custom(str) => format!("({str})").into(),
        }
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<str> {
        Cow::Borrowed(DEFAULT_MULTILINE_INDICATOR)
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<str> {
        let prefix = match history_search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        // NOTE: magic strings, given there is logic on how these compose I am
        // not sure if it is worth extracting in to static constant
        Cow::Owned(format!(
            "({}reverse-search: {}) ",
            prefix, history_search.term
        ))
    }
}
