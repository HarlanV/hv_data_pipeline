import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ShortType, IntegerType, DateType
from common.helpers_tools import latest_run_path


def run(spark, silver_path, bronze_path, bucket_name: str = "hv-challenge") -> None:
    
    # Extract
    database = "silver_mobilidade"
    domain = "mobilidade"
    table_name = "mapa_controle_operacional"
    dataset = "mapa_controle_operacional"
    runs_root_prefix = f"bronze/{dataset}/runs/"
    print(f'Iniciando silver {database}')

    # Consulta a ultima bronze executada
    s3 = boto3.client("s3")
    latest_run_s3 = latest_run_path(s3, bucket_name, runs_root_prefix)
    bronze_df = spark.read.parquet(latest_run_s3)

    # Transform
    transformed_df = bronze_df.select(

        # dd/MM/yyyy -> int yyyymmdd
        F.date_format(
            F.to_date(F.col("viagem"), "dd/MM/yyyy"),
            "yyyyMMdd"
        ).cast("int").alias("data_viagem"),
        F.col("linha").cast(StringType()).alias("numero_linha"),
        F.col("empresa_operadora").cast(StringType()).alias("codigo_empresa"),
        F.col("tipo_dia").cast(StringType()).alias("tipo_dia"),
    )
    # Load
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    full_table_name = f"{database}.{table_name}"

    # Arquitetura: s3://<bucket>/silver/<domain>/<table_name>/
    table_s3_path = f"{silver_path}/{domain}/{table_name}/"

    (
        transformed_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_s3_path)
        .saveAsTable(full_table_name)
    )

    print(f"Silver criada com sucesso: {full_table_name} em {table_s3_path}")