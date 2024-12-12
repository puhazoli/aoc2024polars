import polars as pl
from aoc24 import polars_printer

df = pl.read_csv("day3/day3_input.csv", has_header=False, separator="\t")


def part1(df):
    pattern = r"mul\([0-9]{1,3},[0-9]{1,3}\)"
    df = df.with_columns(pl.col("column_1").str.extract_all(pattern).alias("group"))

    df = df.select(
        pl.col("group").list.explode().str.extract_all(r"[0-9]{1,3}").list.to_struct()
    )
    return (
        df.select(pl.col("group").struct.field("*").cast(pl.Int64))
        .select((pl.col("field_0") * pl.col("field_1")).alias("mul"))
        .select(pl.sum("mul"))
    )


part1(df)


def part2(df):
    df = df.select(pl.col("column_1").str.concat(""))

    df_split = pl.concat(
        [
            df.select(
                pl.col("column_1")
                .str.extract_all(r"do\(\)|don't\(\)")
                .list.explode()
                .alias("doordont")
            ),
            df.select(
                pl.col("column_1")
                .str.replace_all("don't()", "do()")
                .str.split(r"do()")
                .list.explode()
            ),
        ],
        how="horizontal",
    )

    return part1(
        df_split.with_columns(valid_col=pl.col("doordont").shift() == "do()")
        .with_columns(valid_col=pl.col("valid_col").fill_null(True))
        .filter("valid_col")
        .select("column_1")
    )


part2(df)
