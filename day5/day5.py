import polars as pl


def part1():
    df = pl.read_csv(
        "day5/day5.csv",
        has_header=False,
        schema=pl.Schema({"column_1": pl.String()}),
        separator="\n",
    )

    df_rules = (
        df.filter(pl.col("column_1").str.contains("\|"))
        .select(
            pl.col("column_1")
            .str.split("|")
            .list.to_struct(n_field_strategy="max_width")
        )
        .unnest("column_1")
    )

    df_print = (
        df.filter(~pl.col("column_1").str.contains("\|"))
        .with_row_index()
        .with_columns(pl.col("column_1").str.split(","))
        .explode("column_1")
        .with_columns(pl.col("index").len().over("index").alias("len"))
        .with_columns(pl.int_range(pl.len()).over("index").alias("group_ix"))
        .with_columns((pl.col("len") - pl.col("group_ix")).alias("reversed_group_ix"))
    )

    max_periods = f"{df_print.select(pl.max('len')).item()}i"

    before = df_print.rolling(
        index_column="group_ix", period=max_periods, group_by="index"
    ).agg(pl.col("column_1").alias("before"))

    after = (
        df_print.sort(by=["index", "reversed_group_ix"])
        .rolling(index_column="reversed_group_ix", period=max_periods, group_by="index")
        .agg(pl.col("column_1").alias("after"))
    )

    df_print_with_values = df_print.join(
        before, on=["group_ix", "index"], how="left"
    ).join(after, on=["index", "reversed_group_ix"], how="left")

    # .with_columns(pl.col("column_1").rolling())
    valid_row_ix = (
        df_print_with_values.sort(by=["index", "group_ix"])
        .join(df_rules, left_on="column_1", right_on="field_0", how="left")
        .join(df_rules, left_on="column_1", right_on="field_1", how="left")
        .with_columns(
            ~pl.col("field_1").is_in(pl.col("before")).alias("before_check"),
            ~pl.col("field_0").is_in(pl.col("after")).alias("after_check"),
        )
        .filter(
            pl.all("before_check").over("index") | pl.all("after_check").over("index")
        )
        .select("index")
        .unique()
    )

    valid_rows = (
        df.filter(~pl.col("column_1").str.contains("\|"))
        .with_row_index()
        .with_columns(pl.col("column_1").str.split(","))
        .join(valid_row_ix, on="index", how="inner")
    )

    valid_rows.with_columns(pl.col("column_1").list.len().alias("len"))

    return valid_rows.with_columns(
        pl.col("column_1")
        .list.get(pl.col("column_1").list.len() // 2)
        .alias("middle")
        .cast(pl.Int64())
    ).select(pl.sum("middle"))


part1()
