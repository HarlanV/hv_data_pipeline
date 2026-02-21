from tests.checks import check_unique

def run_gold_suite(spark) -> None:
    check_unique(spark, "gold_mobilidade.dim_empresa", "sk_empresa")