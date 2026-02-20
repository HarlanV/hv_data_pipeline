from pyspark.sql import DataFrame


def run(spark, raw_path: str, bronze_path: str) -> None:
    dataset = "mapa_controle_operacional"

    input_path = f"{raw_path}/{dataset}/*.csv"
    output_path = f"{bronze_path}/{dataset}"

    print(f"Lendo arquivos CSV de: {input_path}")

    df: DataFrame = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "false")
        .csv(input_path)
    )

    total_rows = df.count()
    print(f"Total de registros lidos: {total_rows}")

    print(f"Salvando dados em formato parquet em: {output_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("Camada bronze finalizada.")