import polars as pl


def transform(inputs, params):
    df = inputs["enrich_customers"]

    # Compute RFM scores (Frequency + Monetary — Recency omitted since
    # we don't have a reference "now" date in the dataset).
    df = df.with_columns([
        pl.col("rental_count").cast(pl.Float64).alias("frequency"),
        pl.col("total_spent").cast(pl.Float64).alias("monetary"),
    ])

    # Rank into quintiles (1-5)
    df = df.with_columns([
        (pl.col("frequency").rank("ordinal") / pl.col("frequency").count() * 5)
            .cast(pl.Int32).clip(1, 5).alias("f_score"),
        (pl.col("monetary").rank("ordinal") / pl.col("monetary").count() * 5)
            .cast(pl.Int32).clip(1, 5).alias("m_score"),
    ])

    # Combined score and segment label
    df = df.with_columns(
        (pl.col("f_score") + pl.col("m_score")).alias("rfm_score")
    )

    df = df.with_columns(
        pl.when(pl.col("rfm_score") >= 9)
            .then(pl.lit("Champion"))
            .when(pl.col("rfm_score") >= 7)
            .then(pl.lit("Loyal"))
            .when(pl.col("rfm_score") >= 5)
            .then(pl.lit("Potential"))
            .when(pl.col("rfm_score") >= 3)
            .then(pl.lit("At Risk"))
            .otherwise(pl.lit("Dormant"))
            .alias("segment")
    )

    return df
