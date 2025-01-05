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
    common::config::WebDriverConfig, error::WebDriverError, WebDriver,
    WindowHandle,
};
use tokio::runtime::Runtime;
use typeline_core::record_data::custom_data::CustomData;

#[derive(derive_more::Deref, derive_more::DerefMut)]
pub struct SeleniumDriverData {
    #[deref]
    #[deref_mut]
    pub driver: thirtyfour::WebDriver,
    pub active_window: WindowHandle,
}

pub struct SeleniumInstanceData {
    pub process: Child,
    pub runtime: tokio::runtime::Runtime,
    pub driver_data: SeleniumDriverData,
}

#[derive(derive_more::Deref, derive_more::DerefMut)]
pub struct SeleniumInstanceDataWrapper(ManuallyDrop<SeleniumInstanceData>);

#[derive(Clone)]
pub struct SeleniumInstance(pub Arc<Mutex<SeleniumInstanceDataWrapper>>);

#[derive(Clone)]
pub struct SeleniumWindow {
    pub instance: SeleniumInstance,
    pub window_handle: WindowHandle,
}

impl Drop for SeleniumInstanceDataWrapper {
    fn drop(&mut self) {
        let SeleniumInstanceData {
            mut process,
            runtime,
            driver_data,
        } = unsafe { ManuallyDrop::take(&mut self.0) };
        _ = runtime.block_on(async { driver_data.driver.quit().await });
        _ = process.kill();
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

impl SeleniumDriverData {
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

        let data = SeleniumInstanceData {
            runtime,
            process,
            driver_data: SeleniumDriverData {
                driver,
                active_window: window.clone(),
            },
        };

        let instance = SeleniumInstance(Arc::new(Mutex::new(
            SeleniumInstanceDataWrapper(ManuallyDrop::new(data)),
        )));

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
