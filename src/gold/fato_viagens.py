from pyspark.sql import functions as F


def run(spark, gold_path: str):
    print("Iniciando geração da fato_viagem (via Data Catalog)")

    # Catalog definido manualmente

    mco_df = spark.table("silver_mobilidade.seniorerp.mapa_controle_operacional")
    dim_empresa_df = spark.table("silver_mobilidade.seniorerp.dim_empresa")

    fato_viagem = (

        mco_df.alias("operacional")
        .join(
            dim_empresa_df.alias("emp"),
            ["codigo_empresa"],
            "left",
        )
        .select(
            F.col("data_viagem").alias("sk_data"), #datas padronizadas como aaaammdd
            F.col("emp.sk_empresa"),
            F.col("numero_linha").alias("codigo_linha"),
            F.current_timestamp().alias("_data_processamento")
        )
    )

    fato_viagem.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{gold_path}/fato_viagem")

    print("fato_viagem gerada com sucesso")