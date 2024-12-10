import polars as pl


def polars_printer(df: pl.DataFrame) -> None:
    """ "
    Helper function to print non-truncated numbers
    """
    with pl.Config(set_fmt_float="full"):
        print(df)
    return None


def aoc_reader(file_path: str, number_of_cols: int = 2) -> pl.DataFrame:
    """
    Reads in a csv to be compatible with day 1 format
    """
    df = pl.read_csv(file_path, has_header=False)
    df = (
        df.with_columns(
            pl.col("column_1").str.split_exact("   ", n=number_of_cols).alias("col1")
        )
        .unnest("col1")
        .drop("column_1")
        .cast(pl.Float32)
    )
    return df
