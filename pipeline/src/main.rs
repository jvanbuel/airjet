//! airjet sample pipeline: a small Polars ETL step.
//!
//! Compiled both natively (for the KubernetesPodOperator baseline image) and to
//! `wasm32-wasip1` (for the airjet runner). The same source runs in both.
//!
//! Usage:
//!   airjet-pipeline gen <out.csv> <rows>        generate deterministic input data
//!   airjet-pipeline run <in.csv> <out.parquet>  aggregate orders -> parquet
//!   airjet-pipeline noop                        exit immediately (overhead probe)

use std::{env, fs::File, io::Write, process::ExitCode, time::Instant};

use polars_core::prelude::*;
use polars_io::prelude::*;

const REGIONS: [&str; 5] = ["emea", "amer", "apac", "latam", "mea"];
const PRODUCTS: [&str; 8] = [
    "widget", "gadget", "doohickey", "gizmo", "sprocket", "flange", "bracket", "valve",
];

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let res = match args.get(1).map(String::as_str) {
        Some("gen") if args.len() == 4 => generate(&args[2], args[3].parse().unwrap_or(100_000)),
        Some("run") if args.len() == 4 => run(&args[2], &args[3]),
        Some("noop") => Ok(()),
        _ => {
            eprintln!("usage: airjet-pipeline gen <out.csv> <rows> | run <in.csv> <out.parquet> | noop");
            return ExitCode::from(2);
        }
    };
    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Deterministic xorshift so the input is identical across runtimes without a `rand` dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn generate(out: &str, rows: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = Rng(0x5eed_a1e7);
    let mut f = std::io::BufWriter::new(File::create(out)?);
    writeln!(f, "order_id,customer_id,region,product,quantity,unit_price,order_date")?;
    for i in 0..rows {
        let r = rng.next();
        let qty = (r % 20) as i64 - 1; // includes some invalid (<=0) rows to filter out
        let price = 1.0 + (rng.next() % 50_000) as f64 / 100.0;
        let day = rng.next() % 365;
        let (m, d) = month_day(day as u32);
        writeln!(
            f,
            "{i},{},{},{},{qty},{price:.2},2026-{m:02}-{d:02}",
            rng.next() % 10_000,
            REGIONS[(r >> 8) as usize % REGIONS.len()],
            PRODUCTS[(r >> 16) as usize % PRODUCTS.len()],
        )?;
    }
    eprintln!("generated {rows} rows -> {out}");
    Ok(())
}

fn month_day(mut doy: u32) -> (u32, u32) {
    const DAYS: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for (i, n) in DAYS.iter().enumerate() {
        if doy < *n {
            return (i as u32 + 1, doy + 1);
        }
        doy -= n;
    }
    (12, 31)
}

fn run(input: &str, output: &str) -> Result<(), Box<dyn std::error::Error>> {
    let t0 = Instant::now();
    // Read into memory rather than letting polars mmap the file: WASI has no mmap.
    let bytes = std::fs::read(input)?;
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(std::io::Cursor::new(bytes))
        .finish()?;
    let rows_in = df.height();

    // filter out invalid orders
    let mask = df.column("quantity")?.as_materialized_series().i64()?.gt(0);
    let mut df = df.filter(&mask)?;

    // revenue = quantity * unit_price
    let qty = df.column("quantity")?.as_materialized_series().cast(&DataType::Float64)?;
    let price = df.column("unit_price")?.as_materialized_series().clone();
    let revenue = (&qty * &price)?.with_name("revenue".into());
    df.with_column(revenue.into_column())?;

    // month from ISO date string "YYYY-MM-DD"
    let month: Int32Chunked = df
        .column("order_date")?
        .as_materialized_series()
        .str()?
        .iter()
        .map(|o| o.and_then(|s| s.get(5..7)).and_then(|m| m.parse::<i32>().ok()))
        .collect();
    df.with_column(month.with_name("month".into()).into_column())?;

    // group by region/product/month and aggregate
    #[allow(deprecated)] // eager aggregations; the lazy engine does not build for WASI
    let mut out = {
        let gb = df.group_by(["region", "product", "month"])?;
        let mut out = gb.clone().select(["revenue"]).sum()?;
        for agg in [
            gb.clone().select(["order_id"]).count()?,
            gb.clone().select(["quantity"]).mean()?,
            gb.clone().select(["customer_id"]).n_unique()?,
        ] {
            let c = agg.columns().last().unwrap().clone();
            out.with_column(c)?;
        }
        out.sort(["region", "product", "month"], SortMultipleOptions::default())?
    };

    let mut f = File::create(output)?;
    ParquetWriter::new(&mut f).finish(&mut out)?;
    let total: f64 = out.column("revenue_sum")?.as_materialized_series().f64()?.sum().unwrap_or(0.0);
    println!(
        "{{\"rows_in\":{rows_in},\"rows_out\":{},\"total_revenue\":{total:.2},\"elapsed_ms\":{:.1}}}",
        out.height(),
        t0.elapsed().as_secs_f64() * 1e3
    );
    Ok(())
}
