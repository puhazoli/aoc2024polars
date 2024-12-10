import polars as pl
from aoc24 import polars_printer, aoc_reader


def part1():
    df = aoc_reader("day1/day1.csv")
    df = df.select("field_0", "field_1").with_columns(
        r1=pl.col("field_0").rank("ordinal"),
        r2=pl.col("field_1").rank("ordinal"),
    )
    polars_printer(
        pl.concat(
            [
                df.sort("r1").select("field_0"),
                df.sort("r2").select("field_1"),
            ],
            how="horizontal",
        )
        .with_columns(diff=abs((pl.col("field_1") - pl.col("field_0"))))
        .select(pl.sum("diff"))
    )


part1()


def part2():
    df = aoc_reader("day1/day1.csv")
    df_count = df.group_by("field_1").len()
    polars_printer(
        df.select(pl.col("field_0"))
        .join(df_count, left_on="field_0", right_on="field_1")
        .with_columns(sim=pl.col("field_0") * pl.col("len"))
        .select(pl.sum("sim"))
    )


part2()
