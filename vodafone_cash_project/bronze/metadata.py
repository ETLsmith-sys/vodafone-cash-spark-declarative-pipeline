from pyspark.sql import functions as F

def add_metadata(df):
    return (
        df.withColumn(
            "source_file",
            F.col("_metadata.file_path")
        )

        .withColumn(
            "file_date",
            F.to_date(
                F.regexp_extract(
                    "source_file",
                    r"(\d{4}-\d{2}-\d{2})\.csv",
                    1
                )
            )
        )

        .withColumn(
            "year",
            F.year("file_date")
        )

        .withColumn(
            "month",
            F.month("file_date")
        )

        .withColumn(
            "day",
            F.dayofmonth("file_date")
        )

        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )

    )