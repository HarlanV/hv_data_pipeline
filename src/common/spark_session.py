from pyspark.sql import SparkSession


def get_spark():
    spark = (
        SparkSession.builder
        .appName("hv_data_pipeline_glue_5_1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark