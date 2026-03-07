import pandas as pd


if __name__ == "__main__":
    df = pd.read_parquet("silver/2026-03-03/positions-1772586647.parquet.sz")

    print(df)

