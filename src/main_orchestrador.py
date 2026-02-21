import sys
from awsglue.utils import getResolvedOptions

from common.spark_session import get_spark
from common.paths import RAW_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH

from bronze.codigo_interrupcao_viagem import run as bronze_interrupcao_viagem
from bronze.empresa_operadora import run as bronze_operadora
from bronze.mco import run as bronze_mco
from bronze.tipo_dia import run as bronze_tipo_dia

from silver.empresa_operadora import run as silver_empresa_operadora
from silver.interrupcao_viagem import run as silver_interrupcao_viagem
from silver.mapa_controle_operacional import run as silver_mapa_controle_operacional
from silver.tipo_dia import run as silver_tipo_dia

from gold.dim_data import run as dim_data
# from gold.dim_empresa import run as dim_empresa
# from gold.dim_tempo import run as dim_tempo
# from gold.dim_justificativa import run as dim_justificativa
# from gold.fato_viagem import run as fato_viagem


def resolve_layer():
    """
    Resolve argumento --layer de forma opcional.
    Se não for passado, assume 'all'.
    """
    try:
        args = getResolvedOptions(sys.argv, ["layer"])
        layer = args["layer"].lower()
    except Exception:
        layer = "all"

    valid_layers = {"bronze", "silver", "gold", "all"}

    if layer not in valid_layers:
        raise ValueError(
            f"Layer inválida: {layer}. Use bronze, silver, gold ou all."
        )

    return layer


def main():
    layer = resolve_layer()

    print(f"Iniciando execução de main.py | layer={layer}")
    spark = get_spark()

    try:
        # ==================
        # Bronze
        # ==================
        if layer in ("bronze", "all"):
            # bronze_interrupcao_viagem(spark, RAW_PATH, BRONZE_PATH)
            # bronze_operadora(spark, RAW_PATH, BRONZE_PATH)
            bronze_mco(spark, RAW_PATH, BRONZE_PATH)
            # bronze_tipo_dia(spark, RAW_PATH, BRONZE_PATH)

        # ==================
        # Silver
        # ==================
        if layer in ("silver", "all"):
            silver_empresa_operadora(spark, BRONZE_PATH, SILVER_PATH)
            silver_interrupcao_viagem(spark, BRONZE_PATH, SILVER_PATH)
            silver_mapa_controle_operacional(spark, BRONZE_PATH, SILVER_PATH)
            silver_tipo_dia(spark, BRONZE_PATH, SILVER_PATH)

        # ==================
        # Gold
        # ==================
        if layer in ("gold", "all"):
            dim_data(spark, SILVER_PATH, GOLD_PATH)
            # dim_empresa(spark, SILVER_PATH, GOLD_PATH)
            # dim_tempo(spark, SILVER_PATH, GOLD_PATH)
            # dim_justificativa(spark, SILVER_PATH, GOLD_PATH)
            # fato_viagem(spark, SILVER_PATH, GOLD_PATH)

        print("Execução concluída com sucesso.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()