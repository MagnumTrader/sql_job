use std::{error::Error, path::PathBuf, str::FromStr};

pub fn load_env(s: &str)  -> Result<(), Box<dyn Error>>{
    // change to be able to handle different Databases
    // add .envs/.marketeer.env
    let path = PathBuf::from_str(s)?;
    let _ = dotenv::from_path(path)?;
    Ok(())
}

