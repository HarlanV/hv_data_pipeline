import re


def normalize_column(col):
    col = col.replace("\ufeff", "")      # BOM
    col = col.replace("\u00a0", " ")     # NBSP
    col = col.strip()                  # remove espaços início/fim
    col = col.lower()                  # lowercase
    col = re.sub(r"\s+", "_", col)     # espaços internos → _
    # col = re.sub(r"[^\w]", "", col)    # remove caracteres especiais
    return col

def latest_run_path(s3, bucket: str, runs_root_prefix: str) -> str:
    paginator = s3.get_paginator("list_objects_v2")

    runs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=runs_root_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            runs.append(cp["Prefix"])

    if not runs:
        raise RuntimeError(f"Nenhum run encontrado em s3://{bucket}/{runs_root_prefix}")

    latest_prefix = sorted(runs)[-1]  # ordenável pelo padrão YYYY-MM-DDTHH-MM-SSZ
    return f"s3://{bucket}/{latest_prefix}"