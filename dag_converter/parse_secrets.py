"""Secrets backend installed by ``parse_env.parse_environment`` while parsing Dags.

Airflow imports it by path, after the parse environment is set up.
"""

import json
import os
import sys

from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.secrets import BaseSecretsBackend
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.module_loading import import_string

from dag_converter.console_config import print_warning
from dag_converter.parse_env import WRAPPED_BACKEND_ENV, WRAPPED_BACKEND_KWARGS_ENV, state


def _default_given() -> bool:
    """Whether the calling ``airflow.models.Variable.get`` has a default, which Airflow returns for a missing key."""
    frame = sys._getframe(1)
    while frame and not (frame.f_code.co_name == "get" and "default_var" in frame.f_locals):
        frame = frame.f_back
    return bool(frame) and frame.f_locals["default_var"] is not Variable._Variable__NO_DEFAULT_SENTINEL


class ParseSecretsBackend(BaseSecretsBackend):
    """Resolves from the user's backend, AIRFLOW_CONN_*/AIRFLOW_VAR_* and the metastore, else a placeholder."""

    def __init__(self, **kwargs):
        super().__init__()
        self.wrapped = None
        wrapped_path = os.environ.get(WRAPPED_BACKEND_ENV)
        if wrapped_path:
            kwargs = json.loads(os.environ.get(WRAPPED_BACKEND_KWARGS_ENV) or "{}")
            self.wrapped = import_string(wrapped_path)(**kwargs)

    def _from_wrapped(self, method: str, key: str):
        try:
            return getattr(self.wrapped, method)(key) if self.wrapped else None
        except Exception:
            return None

    def _lookup(self, method: str, key: str):
        # Airflow consults this backend first, so check the others here before returning a placeholder
        backends = [self.wrapped] if self.wrapped else []
        backends.append(EnvironmentVariablesBackend())
        if state.db_ready or not state.temp_db_url:
            backends.append(MetastoreBackend())
        for backend in backends:
            try:
                value = getattr(backend, method)(key)
            except Exception:
                continue
            if value is not None:
                return value
        return None

    def get_connection(self, conn_id):
        state.requested_conn_ids.add(conn_id)
        conn = self._lookup("get_connection", conn_id)
        if conn is not None:
            return conn
        if conn_id not in state.stubbed_conn_ids:
            state.stubbed_conn_ids.add(conn_id)
            print_warning(
                f"Connection '{conn_id}' is not defined; using a placeholder Connection for parsing. "
                f"Set AIRFLOW_CONN_{conn_id.upper()} or use a secrets backend to provide one."
            )
        return Connection(
            conn_id=conn_id,
            conn_type="generic",
            host="placeholder",
            login="placeholder",
            password="placeholder",
            schema="placeholder",
            port=1,
        )

    def get_variable(self, key):
        value = self._lookup("get_variable", key)
        # Cosmos treats a missing cosmos_cache__* Variable as a cache miss
        if value is None and not key.startswith("cosmos_cache__") and not _default_given():
            state.stubbed_var_keys.add(key)
            return "placeholder"
        return value

    def get_config(self, key):
        return self._from_wrapped("get_config", key)
