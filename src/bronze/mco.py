from datetime import datetime, timezone
from pyspark.sql import functions as F
from common.history_cleaner import HistoryCleaner
from common.helpers_tools import normalize_column


def run(spark, raw_path: str, bronze_path: str) -> None:
    dataset = "mapa_controle_operacional"
    raw_path = f"{raw_path}/{dataset}/*.csv"
    bronze_path = f"{bronze_path}/{dataset}"

    # Timestamp processamento
    now_utc = datetime.now(timezone.utc)
    run_id = now_utc.strftime("%Y-%m-%dT%H-%M-%SZ")

    print(f"Lendo arquivos CSV de: {raw_path}")
    output_run_path = f"{bronze_path}/runs/{run_id}/"

    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("multiLine", "false")
        .csv(raw_path)
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    df = df.toDF(*[normalize_column(c) for c in df.columns])
    
    # Escreve SOMENTE este run
    (
        df.write
        .mode("overwrite")
        .parquet(output_run_path)
    )

    # Limpa histórico ultimos 3 dias OU 3 execuções (o que for maior)
    cleaner = HistoryCleaner(keep_days=3, keep_min_runs=3)
    cleaner.perform_cleanup(bronze_address=bronze_path, now_ts=now_utc)

    print(f"Bronze OK. Run atual: {run_id}")