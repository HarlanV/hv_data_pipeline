from pyspark.sql import functions as F
from delta.tables import DeltaTable

def run(spark, gold_path: str):
    print("Iniciando geração da dim_empresa")

    database = "gold_mobilidade"
    table_name = "dim_empresa"
    delta_table_path = f"{gold_path}/{table_name}"
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
        .dropDuplicates(["codigo_empresa"])   # importante p/ merge 1:1
    )

    # cria tabela sem identity
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {delta_table_name} (
            sk_empresa BIGINT,
            codigo_empresa STRING,
            nome_empresa STRING,
            _data_processamento TIMESTAMP
        )
        USING DELTA
        LOCATION '{delta_table_path}'
    """)

    # gera SK estável a partir do codigo (reprodutível)
    # xxhash64 retorna long (bigint)
    transformed_df = transformed_df.withColumn("sk_empresa", F.xxhash64("codigo_empresa"))

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
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    print("dim_empresa atualizada com sucesso")