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
    // TODO
    _variant: SeleniumVariant,
    // _client: Client,
}

/*
const DEFAULT_PORT: u16 = 4444;
extern crate fantoccini;
impl SeleniumContext {
    pub async fn new(
        variant: SeleniumVariant,
        port: Option<u16>,
    ) -> Result<SeleniumContext, String> {
        let port = port.unwrap_or(DEFAULT_PORT);
        Ok(SeleniumContext {
            _variant: variant,
            _client: ClientBuilder::native()
                .connect(&format!("http://localhost:{}", port))
                .await
                .map_err(|_| {
                    format!(
                        "failed to connect to webdriver at http://localhost:{}",
                        port
                    )
                })?,
        })
    }
}
*/
