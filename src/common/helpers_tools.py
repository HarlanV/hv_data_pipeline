import re


def normalize_column(col):
    col = col.replace("\ufeff", "")      # BOM
    col = col.replace("\u00a0", " ")     # NBSP
    col = col.strip()                  # remove espaços início/fim
    col = col.lower()                  # lowercase
    col = re.sub(r"\s+", "_", col)     # espaços internos → _
    # col = re.sub(r"[^\w]", "", col)    # remove caracteres especiais
    return col
