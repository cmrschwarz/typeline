use std::{
    borrow::Cow,
    fmt::Debug,
    fs::File,
    io::Cursor,
    mem::ManuallyDrop,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use ini::Ini;
use thirtyfour::{
    common::config::WebDriverConfig, error::WebDriverError,
    support::base64_encode, ElementId, WebDriver, WindowHandle,
};
use tokio::runtime::Runtime;
use typeline_core::record_data::custom_data::CustomData;
use zip::write::FileOptions;

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

fn get_default_firefox_profile() -> Option<PathBuf> {
    let profile_ini_path = if cfg!(windows) {
        dirs_next::config_dir()?.join("Mozilla/Firefox/profiles.ini")
    } else {
        dirs_next::home_dir()?.join(".mozilla/firefox/profiles.ini")
    };

    let conf = Ini::load_from_file(&profile_ini_path).ok()?;

    let mut found_install_section = false;
    let mut best_match = None;

    for (section, properties) in &conf {
        if section.map(|s| s.starts_with("Install")).unwrap_or(false) {
            if let Some(default) = properties.get("Default") {
                found_install_section = true;
                best_match = Some(profile_ini_path.parent()?.join(default));
            }
            continue;
        }
        if found_install_section {
            continue;
        }
        if let Some(default) = properties.get("Default") {
            if default == "1" {
                if let Some(path) = properties.get("Path") {
                    best_match = Some(profile_ini_path.parent()?.join(path));
                }
            }
        }
    }

    best_match
}

fn base64_encode_dir(folder_path: &Path) -> Result<String, std::io::Error> {
    let zip_file = Vec::new();
    let mut zip = zip::ZipWriter::new(Cursor::new(zip_file));
    let options = FileOptions::<()>::default()
        .compression_method(zip::CompressionMethod::Stored);

    fn add_directory(
        zip: &mut zip::ZipWriter<Cursor<Vec<u8>>>,
        options: FileOptions<()>,
        root_path: &Path,
        folder_path: &Path,
    ) -> Result<(), std::io::Error> {
        for entry in std::fs::read_dir(folder_path).unwrap() {
            let entry = entry?;
            let path = entry.path();
            let name = path
                .strip_prefix(root_path)
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "failed to zip directory",
                    )
                })?
                .to_str()
                .unwrap();

            if path.is_file() {
                let mut file = File::open(&path).unwrap();
                zip.start_file(name, options).unwrap();
                std::io::copy(&mut file, zip).unwrap();
            } else if path.is_dir() {
                add_directory(zip, options, root_path, &root_path.join(name))?;
            }
        }
        Ok(())
    }

    add_directory(&mut zip, options, folder_path, folder_path)?;

    let zip_data = zip.finish()?.into_inner();
    Ok(base64_encode(&zip_data))
}

fn start_driver(
    port: &str,
    initial_url: Option<&str>,
    use_profile: bool,
) -> Result<(Runtime, WebDriver, WindowHandle), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| format!("failed to initialize async runtime: `{e}`"))?;

    let mut options = thirtyfour::DesiredCapabilities::firefox();

    if use_profile {
        if let Some(profile) = get_default_firefox_profile() {
            let profile = base64_encode_dir(&profile).map_err(|e| {
                format!("failed to encode firefox profile directory: {e}")
            })?;
            options.set_encoded_profile(&profile).map_err(|e| {
                format!("failed to assign firefox profile: {e}")
            })?;
        }
    }

    let (driver, window) = runtime.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .map_err(|e| format!("failed to create http client: `{e}`"))?;

        let driver = WebDriver::new_with_config_and_client(
            format!("http://localhost:{port}"),
            options,
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
    pub fn new(
        initial_url: Option<&str>,
        use_profile: bool,
    ) -> Result<Self, String> {
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

        let (runtime, driver, window) =
            match start_driver(&port, initial_url, use_profile) {
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
