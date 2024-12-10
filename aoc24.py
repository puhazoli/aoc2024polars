import polars as pl

def polars_printer(df: pl.DataFrame):
    """"
    Helper function to print non-truncated numbers
    """
    with pl.Config(set_fmt_float="full"):
        print(df)
    return None