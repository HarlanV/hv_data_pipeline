from pyspark.sql import functions as F
from delta.tables import DeltaTable


def run(spark, gold_path: str):

    print("Iniciando geração da dim_empresa")

    table_name = "dim_empresa"
    delta_table_path = f"{gold_path}/{table_name}"
    delta_table_name = f"gold_mobilidade.{table_name}"

    # =====================
    # Extract
    # =====================
    empresa_df = spark.table("silver_mobilidade.empresa_operadora")

    transformed_df = (
        empresa_df
        .select(
            F.col("codigo_empresa"),
            F.col("nome_empresa"),
            F.current_timestamp().alias("_data_processamento")
        )
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {delta_table_name} (
            sk_empresa BIGINT GENERATED ALWAYS AS IDENTITY,
            codigo_empresa STRING,
            nome_empresa STRING,
            _data_processamento TIMESTAMP,
            CONSTRAINT pk_sk_empresa PRIMARY KEY (sk_empresa)
        )
        USING DELTA
        LOCATION '{delta_table_path}'
    """)

    delta_table = DeltaTable.forName(spark, delta_table_name)

    (
        delta_table.alias("target")
        .merge(
            source=transformed_df.alias("source"),
            condition="target.codigo_empresa = source.codigo_empresa"
        )
        .whenMatchedUpdate(
            set={
                "nome_empresa": "source.nome_empresa",
                "_data_processamento": "current_timestamp()"
            }
        )
        .whenNotMatchedInsert(
            values={
                "codigo_empresa": "source.codigo_empresa",
                "nome_empresa": "source.nome_empresa",
                "_data_processamento": "current_timestamp()"
            }
        )
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    print("dim_empresa atualizada com sucesso")
