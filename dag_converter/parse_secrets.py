"""Secrets backend installed by ``parse_env.parse_environment`` while parsing Dags.

Airflow imports it by path, after the parse environment is set up.
"""

import json
import os

from airflow.secrets import BaseSecretsBackend
from airflow.secrets.environment_variables import EnvironmentVariablesBackend
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.module_loading import import_string

from dag_converter.console_config import print_warning
from dag_converter.parse_env import WRAPPED_BACKEND_ENV, WRAPPED_BACKEND_KWARGS_ENV, state


class ParseSecretsBackend(BaseSecretsBackend):
    """Resolves from the user's backend, AIRFLOW_CONN_*/AIRFLOW_VAR_* and the metastore."""

    def __init__(self, **kwargs):
        super().__init__()
        self.wrapped = None
        wrapped_path = os.environ.get(WRAPPED_BACKEND_ENV)
        if wrapped_path:
            try:
                kwargs = json.loads(os.environ.get(WRAPPED_BACKEND_KWARGS_ENV) or "{}")
                self.wrapped = import_string(wrapped_path)(**kwargs)
            except Exception as e:
                print_warning(f"Could not start the secrets backend {wrapped_path} ({e}); parsing without it")

    def _from_wrapped(self, method: str, key: str):
        try:
            return getattr(self.wrapped, method)(key) if self.wrapped else None
        except Exception:
            return None

    def _lookup(self, method: str, key: str):
        # Airflow consults this backend first, so check the others here
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
        return self._lookup("get_connection", conn_id)

    def get_variable(self, key):
        return self._lookup("get_variable", key)

    def get_config(self, key):
        return self._from_wrapped("get_config", key)
