use std::{
    borrow::Cow,
    fmt::Debug,
    mem::ManuallyDrop,
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use thirtyfour::{
    common::config::WebDriverConfig, error::WebDriverError, ElementId,
    WebDriver, WindowHandle,
};
use tokio::runtime::Runtime;
use typeline_core::record_data::custom_data::CustomData;

#[derive(derive_more::Deref, derive_more::DerefMut)]
pub struct WindowAwareSeleniumDriver {
    #[deref]
    #[deref_mut]
    pub driver: thirtyfour::WebDriver,
    pub active_window: WindowHandle,
}

pub struct SeleniumInstance {
    pub window_aware_driver: ManuallyDrop<WindowAwareSeleniumDriver>,
    pub process: Child,
    pub runtime: tokio::runtime::Runtime,
}

#[derive(Clone)]
pub struct SeleniumWebElement {
    pub instance: Arc<Mutex<SeleniumInstance>>,
    pub window_handle: WindowHandle,
    pub element_id: ElementId,
}

#[derive(Clone)]
pub struct SeleniumWindow {
    pub instance: Arc<Mutex<SeleniumInstance>>,
    pub window_handle: WindowHandle,
}

impl Drop for SeleniumInstance {
    fn drop(&mut self) {
        let wad = unsafe { ManuallyDrop::take(&mut self.window_aware_driver) };
        _ = self.runtime.block_on(async { wad.driver.quit().await });
        _ = self.process.kill();
    }
}

fn start_driver(
    port: &str,
    initial_url: Option<&str>,
) -> Result<(Runtime, WebDriver, WindowHandle), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| format!("failed to initialize async runtime: `{e}`"))?;
    let (driver, window) = runtime.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .map_err(|e| format!("failed to create http client: `{e}`"))?;

        let driver = WebDriver::new_with_config_and_client(
            format!("http://localhost:{port}"),
            thirtyfour::DesiredCapabilities::firefox(),
            WebDriverConfig::default(),
            client,
        )
        .await
        .map_err(|e| format!("failed to connect to webdriver: `{e}`"))?;
        let window = driver.window().await.map_err(|e| {
            format!("failed to aquire webdriver window: `{e}`")
        })?;
        if let Some(url) = initial_url {
            driver.goto(url).await.map_err(|e| {
                format!("failed to navigate to initial url: `{e}`")
            })?;
        }
        Ok::<(WebDriver, WindowHandle), String>((driver, window))
    })?;

    Ok((runtime, driver, window))
}

impl WindowAwareSeleniumDriver {
    pub async fn switch_to_window(
        &mut self,
        window: &WindowHandle,
    ) -> Result<(), WebDriverError> {
        if &self.active_window == window {
            return Ok(());
        }
        self.driver.switch_to_window(window.clone()).await?;
        self.active_window = window.clone();
        Ok(())
    }
}

fn find_free_port() -> std::io::Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

impl SeleniumWindow {
    pub fn new(initial_url: Option<&str>) -> Result<Self, String> {
        // HACK: This is a TOCTOU bug, but selenium does the same,
        // and there seems to be no other good way besides parsing the
        // geckodriver log output which seems even worse.
        // https://github.com/SeleniumHQ/selenium/blob/1f1d6b9f18de4dfc5bc86f8b25ad88e5b214a217/py/selenium/webdriver/common/service.py#L74
        let port = find_free_port()
            .map_err(|e| {
                format!("failed to find free port for geckodriver: `{e}`")
            })?
            .to_string();

        let mut process = Command::new("geckodriver")
            .args(["--port", &port, "--log", "fatal"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| {
                format!("failed to spawn geckodriver process: `{e}`")
            })?;

        let (runtime, driver, window) = match start_driver(&port, initial_url)
        {
            Ok(res) => res,
            Err(e) => {
                _ = process.kill();
                return Err(e);
            }
        };

        let instance = Arc::new(Mutex::new(SeleniumInstance {
            runtime,
            process,
            window_aware_driver: ManuallyDrop::new(
                WindowAwareSeleniumDriver {
                    driver,
                    active_window: window.clone(),
                },
            ),
        }));

        Ok(SeleniumWindow {
            instance,
            window_handle: window,
        })
    }
}

impl Debug for SeleniumWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SeleniumWindow{..}")
    }
}

impl CustomData for SeleniumWindow {
    fn clone_dyn(
        &self,
    ) -> typeline_core::record_data::custom_data::CustomDataBox {
        Box::new(self.clone())
    }

    fn type_name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("selenium_window")
    }

    fn format(
        &self,
        w: &mut dyn typeline_core::utils::text_write::TextWrite,
        _format: &typeline_core::record_data::formattable::RealizedFormatKey,
    ) -> std::io::Result<()> {
        w.write_all_text("<selenium window>")
    }

    fn cmp(&self, _rhs: &dyn CustomData) -> Option<std::cmp::Ordering> {
        None
    }
}

impl Debug for SeleniumWebElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SeleniumWebElement{..}")
    }
}

impl CustomData for SeleniumWebElement {
    fn clone_dyn(
        &self,
    ) -> typeline_core::record_data::custom_data::CustomDataBox {
        Box::new(self.clone())
    }

    fn type_name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("selenium_web_element")
    }

    fn format(
        &self,
        w: &mut dyn typeline_core::utils::text_write::TextWrite,
        _format: &typeline_core::record_data::formattable::RealizedFormatKey,
    ) -> std::io::Result<()> {
        w.write_all_text("<selenium_web_element>")
    }

    fn cmp(&self, _rhs: &dyn CustomData) -> Option<std::cmp::Ordering> {
        None
    }
}
