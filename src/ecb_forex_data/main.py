from pyspark.sql import SparkSession, DataFrame
from databricks.connect import DatabricksSession
from pyspark.sql.functions import col

def get_taxis(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.nyctaxi.trips")


# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        return DatabricksSession.builder.serverless().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()


def main():
    get_taxis(get_spark()).show(5)



def enrich_with_usd(df):
    usd_df = df.select(col("OBS_VALUE").alias("USD")).filter(
        ( (col("CURRENCY") == "USD") & (col("TIME_PERIOD") == "2025-07-22") )
    )
    df = (
        df.select("TITLE", "CURRENCY", "OBS_VALUE", "CURRENCY_DENOM", "TIME_PERIOD")
        .filter(col("TIME_PERIOD") == "2025-07-22")
        .join(usd_df, how="cross")
        .withColumn("USD", col("OBS_VALUE") / col("USD"))
    )
    return df

if __name__ == "__main__":
    main()

