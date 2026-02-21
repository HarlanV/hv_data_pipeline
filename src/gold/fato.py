import boto3  # (não é usado aqui, mas mantive pra ficar no mesmo padrão da silver)
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, StringType

def run(spark, bucket_name: str = "hv-challenge") -> None:
    # Destino (Gold)
    database = "gold_comum"
    domain = "mobilidade"
    table_name = "dim_data"

    print(f"Iniciando gold {database}.{table_name}")

    # Cria DB e usa
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    full_table_name = f"{database}.{table_name}"

    # Arquitetura: s3://<bucket>/gold/<domain>/<table_name>/
    table_s3_path = f"s3://{bucket_name}/gold/{domain}/{table_name}/"

    # Geração (Spark-only)
    start_date = "2000-01-01"
    end_date = "2040-12-31"

    base_df = spark.sql(f"""
    SELECT explode(
      sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)
    ) AS data
    """)

    date_dim_df = (
        base_df
        .withColumn("sk_data", F.date_format(F.col("data"), "yyyyMMdd").cast("int"))
        .withColumn("ano", F.year("data").cast("int"))
        .withColumn("mes", F.month("data").cast("int"))
        .withColumn("dia", F.dayofmonth("data").cast("int"))
        .withColumn("semana_ano", F.weekofyear("data").cast("int"))
        .withColumn("dia_semana", F.date_format(F.col("data"), "u").cast("int"))  # 1=Mon ... 7=Sun
        .withColumn("fim_de_semana", (F.col("dia_semana") >= 6))
        .withColumn("trimestre", F.quarter("data").cast("int"))
        .withColumn("ultimo_dia_mes", F.dayofmonth(F.last_day("data")).cast("int"))
        .withColumn("mes_ano", F.date_format(F.col("data"), "MM/yyyy"))
        .select(
            "sk_data",
            "data",
            "ano",
            "mes",
            "dia",
            "semana_ano",
            "dia_semana",
            "fim_de_semana",
            "trimestre",
            "ultimo_dia_mes",
            "mes_ano",
        )
    )


    # Load (mesmo padrão da silver)
    (
        date_dim_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_s3_path)
        .saveAsTable(full_table_name)
    )

    print(f"Gold criada com sucesso: {full_table_name} em {table_s3_path}")