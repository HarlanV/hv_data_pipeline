from pyspark.sql import functions as F

def run(spark, gold_path: str):
    print("Iniciando geração da fato_viagem")

    database = "gold_mobilidade"
    table_name = "fato_viagem"
    full_table_name = f"{database}.{table_name}"
    table_path = f"{gold_path}/{table_name}/"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    # Extract
    mco_df = spark.table("silver_mobilidade.mapa_controle_operacional")
    dim_empresa_df = spark.table("gold_mobilidade.dim_empresa")

    # Transform
    fato_viagem = (
        mco_df.alias("operacional")
        .join(dim_empresa_df.alias("emp"), on="codigo_empresa", how="left")
        .select(
            F.col("data_viagem").cast("int").alias("sk_data"),
            F.coalesce(F.col("emp.sk_empresa"), F.lit(-1)).cast("bigint").alias("sk_empresa"),
            F.col("numero_linha").cast("string").alias("codigo_linha"),
            F.current_timestamp().alias("_data_processamento"),
        )
    )

    # Load
    (
        fato_viagem.write
        .format("delta")
        .mode("overwrite")
        .option("path", table_path)
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .partitionBy("sk_data")
        .saveAsTable(full_table_name)
    )

    print(f"{full_table_name} criada com sucesso em {table_path}")