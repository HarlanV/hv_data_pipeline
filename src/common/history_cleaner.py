# common/history_cleaner.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional
import re
import boto3


class HistoryCleaner:
    """
    Limpa histórico de 'runs' no S3.

    Estrutura esperada:
      <bronze_address>/runs/<RUN_ID>/...

    Onde RUN_ID é no formato: YYYY-MM-DDTHH-MM-SSZ (UTC).
    """

    def __init__(self, keep_days: int = 3, keep_min_runs: int = 3, s3_client: Optional[object] = None):
        self.keep_days = keep_days
        self.keep_min_runs = keep_min_runs
        self._set_id_runs()
        self._set_s3_client(s3_client)


    def _set_s3_client(self, s3_client):
        if s3_client is None:
            s3_client = boto3.client("s3")
        self.s3_client = s3_client


    def _set_id_runs(self):
        """
        Formatos padrões para estrutura definida
        """
        self._run_id_format = "%Y-%m-%dT%H-%M-%SZ"  # exemplo: 2026-02-20T12-34-56Z
        self._run_id_regex = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z$")

    @staticmethod
    def _ensure_trailing_slash(prefix: str) -> str:
        return prefix if prefix.endswith("/") else prefix + "/"


    @staticmethod
    def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
        """
        Parseia 's3://bucket/prefix...' -> (bucket, prefix)
        Ex.:  hv-challenge/bronze/mapa_controle_operacional
        """
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"bronze_address deve começar com s3:// (recebido: {s3_uri})")
        
        no_scheme = s3_uri[len("s3://"):]
        parts = no_scheme.split("/", 1)
        bucket = parts[0] # hv-challenge
        prefix = parts[1] if len(parts) > 1 else "" # bronze/mapa_controle_operacional
        
        return bucket, prefix


    def perform_cleanup(self, bronze_address: str, now_ts: datetime) -> None:
        """
        bronze_address: caminho base da bronze do dataset, ex:
          s3://hv-challenge/bronze/mapa_controle_operacional

        now_ts: timestamp atual (datetime). Recomendado timezone-aware (UTC).
        """

        if now_ts.tzinfo is None:
            # assume UTC se vier naive
            now_ts = now_ts.replace(tzinfo=timezone.utc)
        
        bucket, base_prefix = self._parse_s3_uri(bronze_address)
        base_prefix = self._ensure_trailing_slash(base_prefix)
        runs_root_prefix = base_prefix + "runs/"
        run_prefixes = self._list_run_prefixes(bucket, runs_root_prefix)

        # se não tem runs, não faz nada
        if not run_prefixes:
            return

        # calcula quais apagar
        to_delete = self._compute_prefixes_to_delete(run_prefixes, runs_root_prefix, now_ts)

        # apaga
        for prefix in to_delete:
            self._delete_prefix(bucket=bucket, prefix=prefix)


    def _list_run_prefixes(self, bucket: str, runs_root_prefix: str) -> List[str]:
        """
        Lista subpastas dentro de runs_root_prefix usando Delimiter='/' (CommonPrefixes).
        Ex de runs_root_prefix: bronze/mapa_controle_operacional/runs/
        Ex de bucket: hv-challenge
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        prefixes: List[str] = []

        for page in paginator.paginate(Bucket=bucket, Prefix=runs_root_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                prefixes.append(cp["Prefix"])  # ex: bronze/.../runs/2026-...Z/
        return prefixes


    def _delete_prefix(self, bucket: str, prefix: str) -> None:
        """
        Deleta todos os objetos dentro de um prefix.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")

        batch = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})

                if len(batch) == 1000:
                    self.s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                    batch = []

        if batch:
            self.s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})


    def _compute_prefixes_to_delete(
        self,
        run_prefixes: List[str],
        runs_root_prefix: str,
        now_ts: datetime
    ) -> List[str]:
        """
        Regra:
          - manter todos os runs dos últimos keep_days
          - se isso for < keep_min_runs, manter keep_min_runs runs mais recentes
        """
        runs_dt: List[Tuple[str, datetime]] = []
        for rp in run_prefixes:
            dt = self._extract_run_datetime(rp, runs_root_prefix)
            if dt is not None:
                runs_dt.append((rp, dt))

        # se nenhum prefix válido com run_id, não apaga nada (mais seguro)
        if not runs_dt:
            return []

        # mais novo -> mais velho
        runs_dt.sort(key=lambda x: x[1], reverse=True)

        cutoff = now_ts - timedelta(days=self.keep_days)

        keep_set = {rp for rp, dt in runs_dt if dt >= cutoff}

        if len(keep_set) < self.keep_min_runs:
            keep_set = {rp for rp, _ in runs_dt[: self.keep_min_runs]}

        return [rp for rp, _ in runs_dt if rp not in keep_set]


    def _extract_run_datetime(self, run_prefix: str, runs_root_prefix: str) -> Optional[datetime]:
        """
        Extrai RUN_ID do prefix e converte para datetime UTC.
        Espera: run_prefix = <runs_root_prefix>/<RUN_ID>/
        """
        if not run_prefix.startswith(runs_root_prefix):
            return None

        run_id = run_prefix[len(runs_root_prefix):].strip("/")  # "2026-02-20T12-34-56Z"

        if not self._run_id_regex.match(run_id):
            return None

        return datetime.strptime(run_id, self._run_id_format).replace(tzinfo=timezone.utc)