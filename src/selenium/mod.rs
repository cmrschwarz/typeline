use fantoccini::{ClientBuilder, Client};

extern crate  fantoccini;
#[derive(Clone, Copy)]
pub enum SeleniumDownloadStrategy {
    Scr,
    Browser,
    JsFetch,
}

#[derive(Clone, Copy)]
pub enum ChromeVariant {
    Chrome,
    Chromium,
    Opera,
    Edge,
    Brave,
}

#[derive(Clone, Copy)]
pub enum SeleniumVariant {
    Firefox,
    Chrome(ChromeVariant),
    Torbrowser,
}

pub struct SeleniumContext {
    variant: SeleniumVariant,
    client: Client,
}

const DEFAULT_PORT: u16 = 4444;

impl SeleniumContext {
    pub async fn new(variant: SeleniumVariant, port: u16) -> Result<SeleniumContext, String> {
        Ok(SeleniumContext {
            variant:  variant,
            client: ClientBuilder::native().connect(
                &format!("http://localhost:{}", port)
            ).await.map_err(|_| format!("failed to connect to webdriver at http://localhost:{}", port))?
        })
    }
}
