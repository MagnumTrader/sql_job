mod jobs;
mod utils;

use jobs::*;
use utils::load_env;


use clap::Parser;
use sqlx::{
    postgres::PgConnectOptions,
    ConnectOptions,
};

use std::{error::Error, str::FromStr, time::Instant};
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_level(false)
        .with_target(false)
        .init();

    let cli = Cli::parse();
    load_env(&cli.env)?;

    let url = dotenv::var("DATABASE_URL")?;

    let pg_options = PgConnectOptions::from_str(&url)
        .unwrap()
        .disable_statement_logging();

    let mut conn = sqlx::PgPool::connect_with(pg_options).await?;

    info!("Starting job {:?}", cli.job);
    let before = Instant::now();
    cli.job.run(&mut conn).await?;
    info!(
        "Job {:?} done in {:?}",
        cli.job,
        before.elapsed()
    );
    Ok(())
}

///Provide commands for your sql job
#[derive(Parser)]
struct Cli {
    /// Path to your sql script to be ran
    #[command(subcommand)]
    job: Job,
    /// Path to the env variables
    #[arg(long, value_name = "FILE")]
    env: String,
}
