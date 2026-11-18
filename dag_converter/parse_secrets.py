"""Secrets backend installed by ``parse_env.parse_environment`` while parsing Dags.

Airflow imports it by path, after the parse environment is set up.
"""

import json
import os

from airflow.models.connection import Connection
from airflow.secrets import BaseSecretsBackend
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.module_loading import import_string

from dag_converter.console_config import print_warning
from dag_converter.parse_env import WRAPPED_BACKEND_ENV, WRAPPED_BACKEND_KWARGS_ENV, state


class ParseSecretsBackend(BaseSecretsBackend):
    """Resolves --conn/--var overrides, then the user's backend; unknown Connections get a placeholder."""

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

    def get_connection(self, conn_id):
        state.requested_conn_ids.add(conn_id)
        if conn_id in state.conns:
            return Connection(conn_id=conn_id, uri=state.conns[conn_id])
        backends = [self.wrapped] if self.wrapped else []
        backends.append(EnvironmentVariablesBackend())
        if state.db_ready or not state.temp_db_url:
            backends.append(MetastoreBackend())
        for backend in backends:
            try:
                conn = backend.get_connection(conn_id=conn_id)
            except Exception:
                conn = None
            if conn:
                return conn
        if conn_id not in state.stubbed_conn_ids:
            state.stubbed_conn_ids.add(conn_id)
            print_warning(
                f"Connection '{conn_id}' is not defined; using a placeholder Connection for parsing. "
                f"Pass --conn '{conn_id}=<uri>' to provide one."
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
        # Missing Variables are not faked: Airflow then checks the environment and the metadata DB
        if key in state.variables:
            return state.variables[key]
        return self._from_wrapped("get_variable", key)

    def get_config(self, key):
        return self._from_wrapped("get_config", key)
