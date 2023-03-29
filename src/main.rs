use anyhow::{Context, Result};
use clap::Parser;
use csv;
use std::fs;
use tokio;
use tokio_postgres::{Client, NoTls};

static TARGET_DB_URI: &str = "host=localhost dbname=nifty_stocks port=5432 user=postgres";
static VERIFY_CSV_HEADER: &str = "date,close,high,low,open,volume,sma5,sma10,sma15,sma20,ema5,ema10,ema15,ema20,upperband,middleband,lowerband,HT_TRENDLINE,KAMA10,KAMA20,KAMA30,SAR,TRIMA5,TRIMA10,TRIMA20,ADX5,ADX10,ADX20,APO,CCI5,CCI10,CCI15,macd510,macd520,macd1020,macd1520,macd1226,MFI,MOM10,MOM15,MOM20,ROC5,ROC10,ROC20,PPO,RSI14,RSI8,slowk,slowd,fastk,fastd,fastksr,fastdsr,ULTOSC,WILLR,ATR,Trange,TYPPRICE,HT_DCPERIOD,BETA";

#[derive(Debug, Parser)]
struct CLI {
    /// Path for stock files.
    #[clap(short, long = "dir", required = true)]
    dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = CLI::parse();
    let valid_header: Vec<&str> = VERIFY_CSV_HEADER.split(",").collect();
    let valid_header_record = csv::StringRecord::from(valid_header.clone());

    let (client, connection) = tokio_postgres::connect(TARGET_DB_URI, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    verify_connection(&client).await?;

    let paths = fs::read_dir(&cli.dir).unwrap();
    for path in paths {
        let path = path.unwrap().path();
        let file_name = path.file_name().unwrap().to_str().unwrap();
        println!("Reading {file_name}...");

        // Verify the CSV header.
        let file = fs::File::open(&path).unwrap();
        let mut rdr = csv::Reader::from_reader(&file);
        let headers = rdr
            .headers()
            .with_context(|| format!("fetching headers from file: {:?}", file))?;
        if *headers != valid_header_record {
            println!("ERROR: {file_name} has an invalid header: {headers:?}");
        }
        println!("Header valid");

        // Create the table.
        let mut table_name = file_name.clone().to_string();
        table_name.truncate(table_name.len() - 4);
        table_name = sanitise(table_name);

        println!("Creating table {table_name}...");
        create_table(&client, &table_name)
            .await
            .with_context(|| format!("error creating table: {table_name}"))?;

        // Filling data in the table.
        let can = fs::canonicalize(path).unwrap();
        let abs_path = can.to_str().unwrap();
        println!("Filling data from {}", abs_path);
        fill_data(&client, &table_name, &abs_path)
            .await
            .with_context(|| format!("error filling table: {table_name}"))?;

        println!("------------------------------------------------------------------\n\n");
    }

    Ok(())
}

async fn verify_connection(c: &Client) -> anyhow::Result<()> {
    c.execute("select 1", &[]).await?;
    Ok(())
}

async fn create_table(c: &Client, table_name: &str) -> anyhow::Result<()> {
    let definition = include_str!("static/table_definition.sql");
    let query = format!("create table if not exists {table_name} {definition}");
    c.execute(&query, &[])
        .await
        .with_context(|| format!("creating table => {table_name}"))?;
    Ok(())
}

async fn fill_data(c: &Client, table_name: &str, csv_file_path: &str) -> anyhow::Result<()> {
    let query = format!(
        r"
copy {} (date,close,high,low,open,volume,sma5,sma10,sma15,sma20,ema5,ema10,ema15,ema20,upperband,middleband,lowerband,HT_TRENDLINE,KAMA10,KAMA20,KAMA30,SAR,TRIMA5,TRIMA10,TRIMA20,ADX5,ADX10,ADX20,APO,CCI5,CCI10,CCI15,macd510,macd520,macd1020,macd1520,macd1226,MFI,MOM10,MOM15,MOM20,ROC5,ROC10,ROC20,PPO,RSI14,RSI8,slowk,slowd,fastk,fastd,fastksr,fastdsr,ULTOSC,WILLR,ATR,Trange,TYPPRICE,HT_DCPERIOD,BETA)
FROM '{}'
delimiter ','
csv header
",
        table_name, csv_file_path
    );
    c.execute(&query, &[])
        .await
        .with_context(|| format!("filling data => {table_name}"))?;
    Ok(())
}

fn sanitise(mut s: String) -> String {
    s = s.replace("-", "_");
    s = s.replace(" ", "_");
    s = s.replace(".", "_");
    s
}
