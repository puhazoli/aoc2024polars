import polars as pl
from aoc.aoc24 import count_across_cols, move_df
import timeit


def part1(df):
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
    return all


def part2(df):
    df_splitted = df.select(pl.col("column_1").str.split(""))

    df_with_a = (
        df_splitted.with_columns(
            pl.int_ranges(pl.col("column_1").list.len()).alias("ix_list")
        )
        .select(
            "column_1",
            "ix_list",
            pl.col("column_1").list.eval(pl.element() == "A").alias("has_a"),
        )
        .with_row_index()
    )

    df_with_a = df_with_a.with_columns(
        df_with_a.select(pl.col("column_1").alias("shift_backward")).shift(
            n=-1, fill_value=[]
        )
    ).with_columns(
        df_with_a.select(pl.col("column_1").alias("shift_forward")).shift(fill_value=[])
    )

    return (
        df_with_a.explode("ix_list", "has_a")
        .with_columns(
            (pl.col("ix_list") - 1).alias("prev_index"),
            (pl.col("ix_list") + 1).alias("next_index"),
        )
        .with_columns(
            pl.when(pl.col("prev_index") < 0)
            .then(pl.col("column_1").list.len() + 100)
            .otherwise(pl.col("prev_index"))
            .alias("prev_index"),
            pl.when(pl.col("next_index") < 0)
            .then(pl.col("column_1").list.len() + 100)
            .otherwise(pl.col("next_index"))
            .alias("next_index"),
        )
        .with_columns(
            top_left_value=pl.col("shift_backward").list.get(
                pl.col("prev_index"), null_on_oob=True
            ),
            bottom_left_value=pl.col("shift_forward").list.get(
                pl.col("prev_index"), null_on_oob=True
            ),
            top_right_value=pl.col("shift_backward").list.get(
                pl.col("next_index"), null_on_oob=True
            ),
            bottom_right_value=pl.col("shift_forward").list.get(
                pl.col("next_index"), null_on_oob=True
            ),
        )
        .filter("has_a")
        .select(pl.col(r"^*._value$"))
        .drop_nulls()
        .with_columns(pl.concat_list(pl.col("*")).alias("surrounding_letters"))
        .with_columns(
            (pl.col("surrounding_letters").list.count_matches("M")).alias("ms"),
            (pl.col("surrounding_letters").list.count_matches("S")).alias("ss"),
        )
        .filter(
            (pl.col("ms") == 2)
            & (pl.col("ss") == 2)
            & (pl.col("top_left_value") != pl.col("bottom_right_value"))
        )
        .shape[0]
    )


def run():
    df = pl.read_csv("day4/day4.csv", has_header=False)
    part1(df)
    print("Benchmarking part 2...")
    benchmark_iterations = 100
    benchmark_s = timeit.timeit(
        lambda: part2(pl.read_csv("day4/day4.csv", has_header=False)),
        number=benchmark_iterations,
    )
    print(f"Part 2 benchmark: {benchmark_s / benchmark_iterations * 1000 :.2f} ms")


if __name__ == "__main__":
    run()
