import polars as pl
from aoc24 import polars_printer, aoc_reader
from time import perf_counter


def part1():
    df = aoc_reader("day2/day2_input.csv", number_of_cols=7, str_to_split=" ")
    df_transposed_diffs = df.transpose().with_columns(
        pl.col([f"column_{i}" for i in range(df.shape[0])]).diff()
    )
    df_transposed_diffs

    cols = [f"column_{i}" for i in range(1, df.shape[1])]

    df_with_changes = (
        df_transposed_diffs.transpose()
        .drop("column_0")
        .with_columns(
            all_correct_change_pos=pl.all_horizontal(pl.col(cols).is_between(1, 3)),
            all_correct_change_neg=pl.all_horizontal(pl.col(cols).is_between(-3, -1)),
        )
    )
    polars_printer(
        df_with_changes.fill_null(True)
        .with_columns(
            good=pl.col("all_correct_change_pos") | pl.col("all_correct_change_neg")
        )
        .select(pl.sum("good"))
    )


# part1()


def part2():
    df = aoc_reader("day2/day2_input.csv", number_of_cols=7, str_to_split=" ")

    return (
        pl.concat(
            [
                df.with_columns(
                    pl.concat_list([f"field_{i}" for i in lista]).alias(f"{ix}")
                ).select(f"{ix}")
                for ix, lista in enumerate(
                    [[x for x in range(8) if x != j] for j in range(8)]
                )
            ],
            how="horizontal",
        )
        .with_columns(
            [
                pl.col(f"{i}").list.diff().list.drop_nulls().alias(f"{i}")
                for i in range(8)
            ]
        )
        .with_columns(
            [
                pl.col(f"{i}")
                .list.eval((pl.element() >= 1) & (pl.element() <= 3))
                .list.all()
                .alias(f"pos_{i}")
                for i in range(8)
            ]
            + [
                pl.col(f"{i}")
                .list.eval((pl.element() <= -1) & (pl.element() >= -3))
                .list.all()
                .alias(f"neg_{i}")
                for i in range(8)
            ]
        )
    ).select(pl.any_horizontal(pl.col("^pos.*$|^neg.*$")).sum())


t1_start = perf_counter()
part2()
t1_stop = perf_counter()

print("Elapsed time:", t1_stop, t1_start)

print("Elapsed time during the whole program in seconds:", t1_stop - t1_start)
