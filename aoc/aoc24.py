import polars as pl


def polars_printer(df: pl.DataFrame) -> None:
    """ "
    Helper function to print non-truncated numbers
    """
    with pl.Config(set_fmt_float="full"):
        print(df)
    return None


def aoc_reader(
    file_path: str, number_of_cols: int = 2, str_to_split: str = "   "
) -> pl.DataFrame:
    """
    Reads in a csv to be compatible with day 1 format
    """
    df = pl.read_csv(file_path, has_header=False)
    df = (
        df.with_columns(
            pl.col("column_1")
            .str.split_exact(str_to_split, n=number_of_cols)
            .alias("col1")
        )
        .unnest("col1")
        .drop("column_1")
        .cast(pl.Float32)
    )
    return df


def count_across_cols(df, reverse: bool = False) -> pl.DataFrame:
    if reverse:
        df = df.select(pl.col("*").str.reverse())
    return df.select(pl.col("*").str.count_matches("XMAS")).select(
        pl.sum_horizontal(pl.col("*"))
    )


def move_df(df, direction: str = "left") -> pl.DataFrame:
    """ "Shifts the dataframe either left or right"""
    df_pre_moved = (
        df.select(pl.col("column_1").str.split(""))
        .with_row_index()
        .with_columns((df.shape[0] - pl.col("index") - 1).alias("pad"))
    )
    if direction == "right":
        df_moved = df_pre_moved.select(
            (
                pl.concat_list(
                    [
                        pl.lit("0").repeat_by("index"),
                        pl.col("column_1"),
                        pl.lit("0").repeat_by("pad"),
                    ]
                )
            ).alias("moved_list")
        )
    else:
        df_moved = df_pre_moved.select(
            (
                pl.concat_list(
                    [
                        pl.lit("0").repeat_by("pad"),
                        pl.col("column_1"),
                        pl.lit("0").repeat_by("index"),
                    ]
                )
            ).alias("moved_list")
        )

    df_moved = df_moved.select(pl.col("moved_list").list.to_struct())
    df_moved = df_moved.unnest("moved_list").select(pl.col("*").str.concat(""))
    return df_moved
