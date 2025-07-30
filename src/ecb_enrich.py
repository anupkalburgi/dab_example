from pyspark.sql.functions import col

def enrich_with_usd(df, date_filter):
    # Validate date parameter
    if not date_filter or not date_filter.strip():
        raise ValueError("date_filter parameter is required and cannot be empty")
    
    # Validate date format (YYYY-MM-DD)
    try:
        from datetime import datetime
        datetime.strptime(date_filter, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_filter}. Expected format: YYYY-MM-DD")
    
    usd_df = df.select(col("OBS_VALUE").alias("USD")).filter(
        ( (col("CURRENCY") == "USD") & (col("TIME_PERIOD") == date_filter) )
    )
    df = (
        df.select("TITLE", "CURRENCY", "OBS_VALUE", "CURRENCY_DENOM", "TIME_PERIOD")
        .filter(col("TIME_PERIOD") == date_filter)
        .join(usd_df, how="cross")
        .withColumn("USD", col("OBS_VALUE") / col("USD"))
    )
    return df