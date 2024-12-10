import polars as pl


def part1():
    df = pl.read_csv("day1/day1.csv", has_header=False)
    df = df.with_columns(
        pl.col("column_1").str.split_exact("   ", n=2).alias("col1")
    ).unnest("col1")
    df = df.select("field_0", "field_1").with_columns(
        r1=pl.col("field_0").rank("ordinal"),
        r2=pl.col("field_1").rank("ordinal").cast(pl.Float32),
    )
    with pl.Config(set_fmt_float="full"):
        print(
            (
                pl.concat(
                    [
                        df.sort("r1").select("field_0").cast(pl.Float32),
                        df.sort("r2").select("field_1").cast(pl.Float32),
                    ],
                    how="horizontal",
                )
                .with_columns(diff=abs((pl.col("field_1") - pl.col("field_0"))))
                .select(pl.sum("diff"))
            )
        )


part1()


def part2():
    df = pl.read_csv("day1/day1.csv", has_header=False)
    df = df.with_columns(
        pl.col("column_1").str.split_exact("   ", n=2).alias("col1")
    ).unnest("col1")
    df_count = df.group_by("field_1").len()
    with pl.Config(set_fmt_float="full"):
        print(
            df.select(pl.col("field_0"))
            .join(df_count, left_on="field_0", right_on="field_1")
            .with_columns(sim=pl.col("field_0").cast(pl.Float32) * pl.col("len"))
            .select(pl.sum("sim"))
        )


part2()
