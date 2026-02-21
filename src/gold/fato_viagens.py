from pyspark.sql import functions as F


def run(spark, gold_path: str):
    print("Iniciando geração da fato_viagem (via Data Catalog)")

    # Catalog definido manualmente

    mco_df = spark.table("silver_mobilidade.seniorerp.mapa_controle_operacional")

    # Aqui podemos aplicar regras de negocio mais complexas.
    # Nesse desafio foi focado mais na arquitetura e menos em tratamento complexos
    # pra isso 
    fato_viagem = (

        mco_df.alias("operacional")
        .select(
            F.col("data_viagem").alias("sk_data"),
            F.col("LINHA").alias("linha_codigo"),
            F.col("VIAGEM").alias("viagem_codigo"),
            F.col("KM_RODADO").cast("double").alias("km_rodado"),
            F.col("PASSAGEIROS").cast("int").alias("qtd_passageiros"),
            F.current_timestamp().alias("_data_processamento")
        )
    )

    fato_viagem.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{gold_path}/fato_viagem")

    print("fato_viagem gerada com sucesso")