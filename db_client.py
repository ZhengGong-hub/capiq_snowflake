from __future__ import annotations
 
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from time import sleep
from typing import Any
import os
 
import pandas as pd
import polars as pl
import requests
from dotenv import dotenv_values

config = dotenv_values(".env") 
 
@dataclass(frozen=True)
class SnowflakeApiClientConfig:
    account_url: str  # e.g. "https://cmc68710.us-east-1.snowflakecomputing.com"
    token: str  # OAuth bearer token
    warehouse: str
    database: str
    schema: str
    role: str
    timeout_seconds: int = 60
    poll_interval_seconds: float = 0.1
 
class SnowflakeApiClient():
    """
    Minimal Snowflake client exposing `fetch(sql)`.
 
    - Uses Snowflake SQL API /api/v2/statements
    - Handles async execution and all partitions
    """
 
    def __init__(self, config: SnowflakeApiClientConfig) -> None:
        self._config = config
        self._base_url = config.account_url.rstrip("/")
        self._statements_url = f"{self._base_url}/api/v2/statements"
 
        self._headers = {
            "Authorization": f"Bearer {config.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
 
    def _base_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timeout": self._config.timeout_seconds,
            "database": self._config.database,
            "schema": self._config.schema,
            "warehouse": self._config.warehouse,
            "parameters": {"ROWS_PER_RESULTSET": 1_000_000},  # 1M is the limit
        }
        if self._config.role:
            payload["role"] = self._config.role
        return payload
 
    def _raise_for_api_error(
        self,
        response: requests.Response,
        response_json: dict[str, Any],
    ) -> None:
        # 2xx means transport-level success; SQL API uses body fields like 090001 for success too.
        if 200 <= response.status_code < 300:
            return
 
        code = response_json.get("code")
        message = response_json.get("message", "Unknown Snowflake SQL API error")
        raise RuntimeError(f"Snowflake SQL API error {code}: {message}")
 
    def _submit_statement(
        self,
        query: sqlT,
    ) -> tuple[
        requests.Response,
        dict[str, Any],
    ]:
        payload = self._base_payload()
        payload["statement"] = query
 
        resp = requests.post(
            self._statements_url,
            headers=self._headers,
            data=json.dumps(payload),
            timeout=self._config.timeout_seconds,
        )
        resp.raise_for_status()
        res = resp.json()
        self._raise_for_api_error(resp, res)
        return resp, res
 
    def _poll_until_ready(
        self,
        initial_response: requests.Response,
        initial_json: dict[str, Any],
    ) -> dict[str, Any]:
        resp = initial_response
        res = initial_json
 
        while resp.status_code == 202:
            status_url = res.get("statementStatusUrl")
            if not status_url:
                raise RuntimeError(f"No statementStatusUrl in async response: {res}")
 
            if status_url.startswith("http://") or status_url.startswith("https://"):
                poll_url = status_url
            else:
                poll_url = f"{self._base_url}{status_url}"
 
            sleep(self._config.poll_interval_seconds)
 
            resp = requests.get(
                poll_url,
                headers=self._headers,
                timeout=self._config.timeout_seconds,
            )
            resp.raise_for_status()
            res = resp.json()
            self._raise_for_api_error(resp, res)
 
        return res
 
    def _fetch_partition(
        self,
        statement_handle: str,
        partition_id: int,
    ) -> list[list[Any]]:
        url = f"{self._statements_url}/{statement_handle}?partition={partition_id}"
        resp = requests.get(
            url,
            headers=self._headers,
            timeout=self._config.timeout_seconds,
        )
        resp.raise_for_status()
        res = resp.json()
        self._raise_for_api_error(resp, res)
        return res.get("data") or []
 
    def fetch(
        self,
        query,
    ) -> dict[str, Any]:
        # 1) submit and wait until first partition is ready
        resp, res = self._submit_statement(query)
        res = self._poll_until_ready(resp, res)
 
        meta = res.get("resultSetMetaData")
        if not meta:
            raise RuntimeError(f"No resultSetMetaData in response: {res}")
 
        partition_info = meta.get("partitionInfo", [])
        statement_handle = res.get("statementHandle")
        if not statement_handle:
            raise RuntimeError(f"No statementHandle in response: {res}")
 
        num_partitions = len(partition_info)
 
        # 2) collect all rows from all partitions
        all_rows: list[list[Any]] = []
        all_rows.extend(res.get("data") or [])
 
        for partition_id in range(1, num_partitions):
            rows = self._fetch_partition(statement_handle, partition_id)
            if rows:
                all_rows.extend(rows)
 
        # 3) put merged data back into the response json and return it
        res["data"] = all_rows
        return res
 
    def ping(self) -> bool:
        """Return True if the connection and credentials are valid, False otherwise."""
        try:
            resp, res = self._submit_statement("SELECT 1")
            self._poll_until_ready(resp, res)
            return True
        except Exception:
            return False

    def close(self) -> None:
        """
        Close the client.
 
        Snowflake SQL API is stateless HTTP, so there is nothing to actually
        close, but this method exists for interface compatibility.
        """
        # no persistent connection to close
        return None
 
    def execute(
        self,
        query,
    ) -> None:
        """
        Execute a statement that does not need to return rows
        (e.g. DDL or DML). Waits until completion and raises on error.
        """
        resp, res = self._submit_statement(query)
        self._poll_until_ready(resp, res)
        # ignore result; errors are raised in _poll_until_ready
        return None
 
    def res_json_to_pandas(self, res: dict[str, Any]) -> pd.DataFrame:
        row_type = res["resultSetMetaData"]["rowType"]
        data = res.get("data") or []

        col_names = [c["name"] for c in row_type]
        df = pd.DataFrame(data, columns=col_names)

        for col in row_type:
            name = col["name"]
            sf_type = col["type"].lower()
            if sf_type in ("timestamp_ntz", "timestamp_ltz", "timestamp_tz", "timestamp"):
                df[name] = pd.to_datetime(pd.to_numeric(df[name]), unit="s")
        #TODO: handle more Snowflake types as needed (e.g. dates, times, variants)!

        return df
 
