from common.spark_session import get_spark
from common.paths import RAW_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH

from bronze.bronze_mco import run as bronze_mco
# from silver.silver_mapa_controle_oper import run as silver_mapa_controle_oper

# from gold.dim_empresa import run as dim_empresa
# from gold.dim_tempo import run as dim_tempo
# from gold.dim_justificativa import run as dim_justificativa
# from gold.fato_viagem import run as fato_viagem


def main():
    spark = get_spark()

    bronze_mco(spark, RAW_PATH, BRONZE_PATH)
    # silver_mapa_controle_oper(spark, BRONZE_PATH, SILVER_PATH)

    # dim_empresa(spark, SILVER_PATH, GOLD_PATH)
    # dim_tempo(spark, SILVER_PATH, GOLD_PATH)
    # dim_justificativa(spark, SILVER_PATH, GOLD_PATH)

    # fato_viagem(spark, SILVER_PATH, GOLD_PATH)

    spark.stop()


if __name__ == "__main__":
    main()