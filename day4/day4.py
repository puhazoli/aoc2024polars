import polars as pl

df = pl.read_csv("day4/day4.csv", has_header=False)


def count_across_cols(df, reverse: bool = False):
    if reverse:
        df = df.select(pl.col("*").str.reverse())
    return df.select(pl.col("*").str.count_matches("XMAS")).select(
        pl.sum_horizontal(pl.col("*"))
    )


def move_df(df, direction: str = "left"):
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


# Find normal
left_to_right = df.select(pl.col("column_1").str.count_matches("XMAS")).select(
    pl.sum("column_1")
)
# Find backwards
right_to_left = df.select(
    pl.col("column_1").str.reverse().str.count_matches("XMAS")
).select(pl.sum("column_1"))
# Find top to bottom
df_split = (
    df.select(pl.col("column_1").str.split("").list.to_struct())
    .unnest("column_1")
    .select(pl.col("*").str.concat(""))
)
top_to_bottom = count_across_cols(df_split)
# and bottom to top
bottom_to_top = count_across_cols(df_split, reverse=True)

df_moved_to_right = move_df(df, direction="right")
df_moved_to_left = move_df(df, direction="left")

top_left_to_bottom_right = count_across_cols(df_moved_to_right)
bottom_right_to_top_left = count_across_cols(df_moved_to_right, reverse=True)

tr_bl = count_across_cols(df_moved_to_left)
bl_tr = count_across_cols(df_moved_to_left, reverse=True)


all = (
    left_to_right
    + right_to_left
    + top_to_bottom
    + bottom_to_top
    + top_left_to_bottom_right
    + bottom_right_to_top_left
    + tr_bl
    + bl_tr
)
print(all)
