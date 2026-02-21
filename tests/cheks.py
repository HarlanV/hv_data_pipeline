from pyspark.sql import functions as F

def check_unique(spark, table: str, col: str, sample: int = 50) -> None:
    df = spark.table(table).select(F.col(col))
    total = df.count()
    distinct = df.distinct().count()

    if total != distinct:
        dups = (
            spark.table(table)
            .groupBy(col)
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
            .limit(sample)
        )
        dups.show(truncate=False)
        raise RuntimeError(f"[TEST FAIL] {table}.{col} duplicado (total={total}, distinct={distinct})")

    print(f"[TEST OK] {table}.{col} é único (total={total})")