from pyspark.sql import functions as F
from delta.tables import DeltaTable


def run(spark, gold_path: str, silver_path:str):
    print("Iniciando geração da dim_empresa")

    database = "gold_mobilidade"
    table_name = "dim_empresa"

    delta_table_path = f"{gold_path}/{table_name}/"
    delta_table_name = f"{database}.{table_name}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    empresa_df = spark.table("silver_mobilidade.empresa_operadora")

    transformed_df = (
        empresa_df
        .select(
            F.col("codigo_empresa").cast("string"),
            F.col("nome_empresa").cast("string"),
            F.current_timestamp().alias("_data_processamento")
        )
        .dropDuplicates(["codigo_empresa"])
        .withColumn("sk_empresa", F.xxhash64("codigo_empresa"))
        .select("sk_empresa", "codigo_empresa", "nome_empresa", "_data_processamento")
    )

    # Cria tabela, caso não exista
    if not spark.catalog.tableExists(delta_table_name):
        (
            transformed_df.limit(0)
            .write.format("delta")
            .mode("overwrite")
            .option("path", delta_table_path)
            .saveAsTable(delta_table_name)
        )

    delta_table = DeltaTable.forName(spark, delta_table_name)

    (
        delta_table.alias("target")
        .merge(
            source=transformed_df.alias("source"),
            condition="target.codigo_empresa = source.codigo_empresa"
        )
        .whenMatchedUpdate(
            set={
                "sk_empresa": "source.sk_empresa",
                "nome_empresa": "source.nome_empresa",
                "_data_processamento": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "sk_empresa": "source.sk_empresa",
                "codigo_empresa": "source.codigo_empresa",
                "nome_empresa": "source.nome_empresa",
                "_data_processamento": "current_timestamp()"
            }
        )
        # NÃO deletar a UNKNOWN
        .whenNotMatchedBySourceDelete(condition="target.sk_empresa <> -1")
        .execute()
    )

    # Garante a empresa desconhecida (sk = -1) na dimensão
    unknown_df = spark.createDataFrame(
        [(-1, "UNKNOWN", "Empresa Desconhecida")],
        ["sk_empresa", "codigo_empresa", "nome_empresa"]
    ).withColumn("_data_processamento", F.current_timestamp())

    (
        DeltaTable.forName(spark, delta_table_name)
        .alias("target")
        .merge(
            unknown_df.alias("source"),
            "target.sk_empresa = source.sk_empresa"
        )
        .whenMatchedUpdate(
            set={
                "codigo_empresa": "source.codigo_empresa",
                "nome_empresa": "source.nome_empresa",
                "_data_processamento": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "sk_empresa": "source.sk_empresa",
                "codigo_empresa": "source.codigo_empresa",
                "nome_empresa": "source.nome_empresa",
                "_data_processamento": "current_timestamp()"
            }
        )
        .execute()
    )

    print("dim_empresa atualizada com sucesso (incluindo UNKNOWN)")