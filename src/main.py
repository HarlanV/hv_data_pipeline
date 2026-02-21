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


def main(): 

    print("Iniciando execução de main.py (entrypoint)")
    spark = get_spark()
    # spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # Bronze [check]
    bronze_interrupcao_viagem(spark, RAW_PATH, BRONZE_PATH)
    # bronze_operadora(spark, RAW_PATH, BRONZE_PATH)
    bronze_mco(spark, RAW_PATH, BRONZE_PATH)
    # bronze_tipo_dia(spark, RAW_PATH, BRONZE_PATH)
    
    # Silver [doing]
    silver_empresa_operadora(spark, SILVER_PATH)
    silver_interrupcao_viagem(spark, SILVER_PATH)
    silver_mapa_controle_operacional(spark, SILVER_PATH)
    silver_tipo_dia(spark, SILVER_PATH)
    
    # Gold[pendente]
    dim_data(spark, GOLD_PATH)
    # dim_empresa(spark, SILVER_PATH, GOLD_PATH)
    # dim_tempo(spark, SILVER_PATH, GOLD_PATH)
    # dim_justificativa(spark, SILVER_PATH, GOLD_PATH)

    # fato_viagem(spark, SILVER_PATH, GOLD_PATH)

    spark.stop()
    print("execução de main concluida")


if __name__ == "__main__":
    main()