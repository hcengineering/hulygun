use std::{num::NonZero, path::Path, sync::LazyLock};

use config::FileFormat;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub group_id: String,
    pub topics: Vec<String>,
    pub service_id: String,
    pub dry_run: bool,
    pub rate_limit: NonZero<u32>,
}

impl Config {
    pub fn topics(&self) -> Vec<&str> {
        self.topics.iter().map(String::as_str).collect()
    }
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    const DEFAULTS: &str = r#"
        group_id = "hulygun"
        topics = ["commevent"]
        service_id = "hulygun"
        dry_run = false
        rate_limit = 10 # messages per second
    "#;

    let mut builder =
        config::Config::builder().add_source(config::File::from_str(DEFAULTS, FileFormat::Toml));

    let path = Path::new("etc/config.toml");

    if path.exists() {
        builder = builder.add_source(config::File::with_name(path.as_os_str().to_str().unwrap()));
    }

    let settings = builder
        .add_source(config::Environment::with_prefix("HULY"))
        .build()
        .and_then(|c| c.try_deserialize::<Config>());

    match settings {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("configuration error: {}", error);
            std::process::exit(1);
        }
    }
});
