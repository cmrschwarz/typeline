use std::{
    borrow::Cow,
    fmt::Debug,
    mem::ManuallyDrop,
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
    pub window: WindowHandle,
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
        Ok::<(WebDriver, WindowHandle), String>((driver, window))
    })?;

    Ok((runtime, driver, window))
}

impl Debug for SeleniumInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SeleniumInstance{..}")
    }
}

impl CustomData for SeleniumInstance {
    fn clone_dyn(
        &self,
    ) -> typeline_core::record_data::custom_data::CustomDataBox {
        Box::new(self.clone())
    }

    fn type_name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("selenium_instance")
    }

    fn format(
        &self,
        w: &mut dyn typeline_core::utils::text_write::TextWrite,
        _format: &typeline_core::record_data::formattable::RealizedFormatKey,
    ) -> std::io::Result<()> {
        w.write_all_text("<selenium instance>")
    }
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

impl SeleniumWindow {
    pub fn new() -> Result<Self, String> {
        // TODO: find free port or let geckodriver choose and parse logs :/
        let port = "5555";
        let mut process = Command::new("geckodriver")
            .args(["--port", port])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| {
                format!("failed to spawn geckodriver process: `{e}`")
            })?;

        let (runtime, driver, window) = match start_driver(port) {
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

        Ok(SeleniumWindow { instance, window })
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
