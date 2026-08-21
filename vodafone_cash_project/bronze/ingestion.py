from pyspark import pipelines as dp 
from schema import bronze_schema
from metadata import add_metadata
from vodafone_cash_project.common.config import DATA_SOURCE


@dp.materialized_view(name="bronze.raw_transactions")
def raw_transactions():
    df = (spark.read.format("csv")
        .option("header", "true")
        .schema(bronze_schema)
        .load(DATA_SOURCE))
    return add_metadata(df)
    