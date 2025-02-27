//!All the jobs are defined here
//!Starting with the [`Job`] Subcommand
//!
//!Then comes the actual jobs, structured
//!Job function -> support functions / structs
use std::error::Error;

use chrono::{DateTime, Utc};
use clap::{Subcommand, ValueEnum};
use sqlx::{Executor, PgPool, Row};
use tokio::task::JoinSet;
use tracing::error;

/// Covering all the available jobs,
/// call run on the job variant to run the associated function.
#[derive(Debug, Clone, Subcommand)]
pub enum Job {
    /// Uses table with Tickdata och truncates to the chosen timeframe.
    TickAggrigate { timeframe: OhlcTimeframe },
}

impl Job {
    pub async fn run(&self, pool: &PgPool) -> Result<(), Box<dyn Error>> {
        match self {
            Job::TickAggrigate { timeframe } => aggrigate_ticks(pool, timeframe).await?,
        };

        Ok(())
    }
}

/// Aggrigate all the ticks to a chosen timeframe.
/// # usage
/// Call using [`Job::run()`] on a [`Job::TickAggrigate`] variant.
/// ```no_run
///     let job = Job::TickAggrigate {OhlcTimeframe::Hour};
///     job.run().await;
///```
async fn aggrigate_ticks(pool: &PgPool, timeframe: &OhlcTimeframe) -> Result<(), sqlx::Error> {
    let (timeframe, table) = match timeframe {
        OhlcTimeframe::Minute => ("minute", "minute_ohlc_data"),
        OhlcTimeframe::Hour => ("hour", "hour_ohlc_data"),
        OhlcTimeframe::Daily => ("day", "daily_ohlc_data"),
    };

    //TODO: On a rainy day, use query to recieve only the latest timestamp (order by timestamp desc limit 1)
    let query = format!(
        "with tickers as (select distinct ticker from raw_price_data)
select tickers.ticker, max(timestamp) as latest from tickers
left join {table} on {table}.ticker = tickers.ticker
group by tickers.ticker
"
    );

    let tickers_and_latest_timestamps = sqlx::query(&query).fetch_all(pool).await.unwrap();

    let mut set = JoinSet::new();

    for row in tickers_and_latest_timestamps.into_iter() {
        let ticker: String = row.get(0);
        let last_bar_date = row.get::<Option<DateTime<Utc>>, _>(1);

        let query = get_truncate_query(&ticker, table, timeframe, last_bar_date);

        let mut local_pool = pool.acquire().await?;

        set.spawn(async move {
            // unwrap because if the query fails i want the task to fail and emit the error
            local_pool.execute(query.as_str()).await.unwrap();
        });
    }

    while let Some(t) = set.join_next().await {
        match t {
            Ok(_) => {}
            Err(e) => error!("error when joining task, {}", e),
        };
    }

    Ok(())
}

fn get_truncate_query(
    ticker: &str,
    table: &str,
    timeframe: &str,
    last_bar_date: Option<DateTime<Utc>>,
) -> String {
    let latest = match last_bar_date {
        Some(timestamp) => format!("and timestamp >= date '{}'", timestamp.date_naive()),
        None => String::from(""),
    };

    format!(
        r#"
with ticks as (
    select 
        timestamp,
        date_trunc('{timeframe}', timestamp) as minute_timestamp,
        ticker,
        price,
        first_value(price) over (partition by date_trunc('{timeframe}', timestamp) order by timestamp) as open,
        first_value(price) over (partition by date_trunc('{timeframe}', timestamp) order by timestamp desc) as close,
        volume
    from 
        raw_price_data
    where ticker = '{ticker}' {latest}
    order by timestamp desc
),

truncated_ticks as (select 
    ticker, 
    minute_timestamp, 
    max(open) as open,
    max(price) as high, 
    min(price) as low,
    max(close) as close,
    sum(volume) as volume
from ticks 
group by ticker, minute_timestamp
order by minute_timestamp desc)
-- insert stuff variable here  v after the quote
INSERT INTO {table} (ticker, timestamp, open, high, low, close, volume) 
select ticker, minute_timestamp as timestamp, open, high, low, close, volume from truncated_ticks
on conflict (ticker, timestamp) do update
set open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume;
    "#
    )
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OhlcTimeframe {
    Minute,
    Hour,
    Daily,
}
