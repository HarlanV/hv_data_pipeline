import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ShortType, IntegerType, DateType


def _latest_run_path(bucket: str, runs_root_prefix: str) -> str:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    runs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=runs_root_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            runs.append(cp["Prefix"])

    if not runs:
        raise RuntimeError(f"Nenhum run encontrado em s3://{bucket}/{runs_root_prefix}")

    latest_prefix = sorted(runs)[-1]  # ordenável pelo padrão YYYY-MM-DDTHH-MM-SSZ
    return f"s3://{bucket}/{latest_prefix}"


def run(spark, bucket_name: str = "hv-challenge") -> None:
    
    # Extract
    database = "silver_mobilidade"
    domain = "mobilidade"
    table_name = "empresa_operadora"
    dataset = "empresa_operadora"
    runs_root_prefix = f"bronze/{dataset}/runs/"
    print(f'Iniciando silver {database}')

    # Consulta a ultima bronze executada
    latest_run_s3 = _latest_run_path(bucket_name, runs_root_prefix)
    bronze_df = spark.read.parquet(latest_run_s3)

    # Transform
    transformed_df = bronze_df.select(
        #Obs: aqui o tipo_de_dia estava assim no pdf original, não é erro de modelagem
        F.col("tipo_de_dia").cast(IntegerType()).alias("codigo_empresa"),
        F.col("descricao").cast(StringType()).alias("nome_empresa"),
    )
    # Load
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    full_table_name = f"{database}.{table_name}"

    # Arquitetura: s3://<bucket>/silver/<domain>/<table_name>/
    table_s3_path = f"s3://{bucket_name}/silver/{domain}/{table_name}/"

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