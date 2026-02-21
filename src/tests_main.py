from awsglue.context import GlueContext
from pyspark.context import SparkContext
from tests.suites import run_gold_suite

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

run_gold_suite(spark)