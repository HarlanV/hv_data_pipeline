from pyspark.sql import functions as F


def run(spark, gold_path: str, silver_path:str):
    database = "gold_comum"
    domain = "mobilidade"
    table_name = "dim_data"

    print(f"Iniciando gold {database}.{table_name}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    full_table_name = f"{database}.{table_name}"
    table_s3_path = f"{gold_path}/{domain}/{table_name}/"

    start_date = "2000-01-01"
    end_date   = "2040-12-31"

    # Evita parsing no SQL e define formato explicitamente
    start = F.to_date(F.lit(start_date), "yyyy-MM-dd")
    end   = F.to_date(F.lit(end_date),   "yyyy-MM-dd")

    base_df = spark.range(1).select(
        F.explode(F.sequence(start, end, F.expr("interval 1 day"))).alias("data")
    )

    # dayofweek: 1=Sun ... 7=Sat
    # Quer 1=Mon ... 7=Sun -> converte:
    dia_semana_iso = F.expr("((dayofweek(data) + 5) % 7) + 1")

    date_dim_df = (
        base_df
        .withColumn("sk_data", F.date_format("data", "yyyyMMdd").cast("int"))
        .withColumn("ano", F.year("data").cast("int"))
        .withColumn("mes", F.month("data").cast("int"))
        .withColumn("dia", F.dayofmonth("data").cast("int"))
        .withColumn("semana_ano", F.weekofyear("data").cast("int"))
        .withColumn("dia_semana", dia_semana_iso.cast("int"))  # 1=Mon ... 7=Sun
        .withColumn("fim_de_semana", (F.col("dia_semana") >= 6))
        .withColumn("trimestre", F.quarter("data").cast("int"))
        .withColumn("ultimo_dia_mes", F.dayofmonth(F.last_day("data")).cast("int"))
        .withColumn("mes_ano", F.date_format("data", "MM/yyyy"))
        .withColumn("_data_processamento", F.current_timestamp())
        .select(
            "sk_data","data","ano","mes","dia","semana_ano","dia_semana",
            "fim_de_semana","trimestre","ultimo_dia_mes","mes_ano", "_data_processamento"
        )    
    )

    (
        date_dim_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_s3_path)
        .saveAsTable(full_table_name)
    )

    print(f"Gold criada com sucesso: {full_table_name} em {table_s3_path}")