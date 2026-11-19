# /// script
# requires-python = ">=3.11"
# dependencies = ["polars==2.1.0"]
# ///
"""Python/Polars twin of pipeline/src/main.rs, used by the uv-based variants.

Usage:
  pipeline.py run <in.csv> <out.parquet>
  pipeline.py noop
"""
import json
import sys
import time


def run(src: str, dst: str) -> None:
    import polars as pl

    t0 = time.perf_counter()
    df = pl.read_csv(src)
    out = (
        df.filter(pl.col("quantity") > 0)
        .with_columns(
            revenue=pl.col("quantity").cast(pl.Float64) * pl.col("unit_price"),
            month=pl.col("order_date").str.slice(5, 2).cast(pl.Int32),
        )
        .group_by("region", "product", "month")
        .agg(
            revenue_sum=pl.col("revenue").sum(),
            order_id_count=pl.col("order_id").count(),
            quantity_mean=pl.col("quantity").mean(),
            customer_id_n_unique=pl.col("customer_id").n_unique(),
        )
        .sort("region", "product", "month")
    )
    out.write_parquet(dst)
    print(json.dumps({
        "rows_in": df.height,
        "rows_out": out.height,
        "total_revenue": round(out["revenue_sum"].sum(), 2),
        "elapsed_ms": round((time.perf_counter() - t0) * 1e3, 1),
    }))


if __name__ == "__main__":
    match sys.argv[1:]:
        case ["run", src, dst]:
            run(src, dst)
        case ["noop"]:
            pass
        case _:
            sys.exit(__doc__)
