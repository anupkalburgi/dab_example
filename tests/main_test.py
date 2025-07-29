from ecb_forex_data.main import get_taxis, get_spark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from ecb_forex_data.main import enrich_with_usd  

def test_main():
    taxis = get_taxis(get_spark())
    assert taxis.count() > 5


# Replace 'your_module' with the actual module name

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()

def test_enrich_with_usd(spark):
    test_data = [
        # ("Title1", "USD", 10.0, "EUR", "2025-07-22"),
        ("Title2", "USD", 20.0, "EUR", "2025-07-22"),
        ("Title3", "GBP", 30.0, "EUR", "2025-07-22")
    ]
    test_df = spark.createDataFrame(
        test_data, 
        ["TITLE", "CURRENCY", "OBS_VALUE", "CURRENCY_DENOM", "TIME_PERIOD"]
    )
    
    result_df = enrich_with_usd(test_df)
    
    expected_data = [
        # ("Title1", "USD", 10.0, "EUR", "2025-07-22", 1.0),
        ("Title2", "USD", 20.0, "EUR", "2025-07-22", 1.0),
        ("Title3", "GBP", 30.0, "EUR", "2025-07-22", 1.5)
    ]
    expected_df = spark.createDataFrame(
        expected_data, 
        ["TITLE", "CURRENCY", "OBS_VALUE", "CURRENCY_DENOM", "TIME_PERIOD", "USD"]
    )
    
    assert result_df.collect() == expected_df.collect()