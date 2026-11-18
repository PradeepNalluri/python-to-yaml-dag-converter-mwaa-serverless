"""Tests for compat_checks module."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dag_converter import compat_checks
from dag_converter.compat_checks import env_key_problem


class TestCompatChecks(unittest.TestCase):
    def setUp(self):
        compat_checks._warned.clear()

    def test_env_key_rules(self):
        self.assertIsNone(env_key_problem("MY_SETTING"))
        self.assertIsNone(env_key_problem("AIRFLOW__WORKERS__SECRETS_BACKEND"))
        self.assertIsNone(env_key_problem("AIRFLOW_CONN_MY_DB"))
        for key in ["AWS_REGION", "AIRFLOW__CORE__X", "PATH", "AIRFLOW_HOME", "LC_CTYPE"]:
            self.assertIn("reserved", env_key_problem(key), key)
        for key in ["MY-KEY", "1ABC", "_X"]:
            self.assertIn("not a valid", env_key_problem(key), key)

    def test_env_var_usage_in_source(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "dag.py"
            source.write_text(
                "import os\nfrom os import getenv, environ\n"
                "a = os.environ['AIRFLOW_HOME']\nb = os.getenv('MY-KEY')\nc = environ.get('OK_KEY')\n"
                "d = getenv('AWS_REGION', 'x')\ne = os.environ.get(name)\nos.environ.setdefault('AWS_X', 'y')\n"
            )
            with patch.object(compat_checks, "print_warning") as warn:
                compat_checks.check_env_var_usage([source])
        messages = " ".join(call.args[0] for call in warn.call_args_list)
        self.assertEqual(warn.call_count, 3)
        for key in ["'AIRFLOW_HOME'", "'MY-KEY'", "'AWS_REGION'"]:
            self.assertIn(key, messages)

    def test_runtime_references(self):
        dag = {
            "d": {
                "tasks": {
                    "t": {
                        "conn_id": "my-db",
                        "aws_conn_id": "aws_default",
                        "profile_config": {"profile_mapping": {"__type__": "x.Mapping", "conn_id": "dbt-conn"}},
                        "bash_command": "echo {{ conn.pg.host }} {{ conn.get('my-api').login }} conn.not_jinja",
                        "sql": "{{ conn['c-3'].host }}",
                        "env": {
                            "A": "{{ var.value.env }}-{{ var.value.get('mode', 'a') }}-{{ var.json.cfg.x }}",
                            "B": "{{ var.value['k-1'] }}",
                        },
                    }
                }
            }
        }
        conn_ids, var_keys = compat_checks.runtime_references(dag)
        self.assertEqual(conn_ids, {"my-db", "dbt-conn", "pg", "my-api", "c-3"})
        self.assertEqual(var_keys, {"env", "mode", "cfg", "k-1"})

    def test_runtime_notice(self):
        with patch.object(compat_checks, "print_warning") as warn:
            compat_checks.print_runtime_notice({"aws_default", "duckdb_default"}, set())
        warn.assert_not_called()
        with patch.object(compat_checks, "print_warning") as warn:
            compat_checks.print_runtime_notice({"aws_default", "my-db", "pg"}, {"env"})
        warn.assert_called_once()
        notice = warn.call_args[0][0]
        self.assertNotIn("aws_default", notice)
        for text in [
            "airflow/connections/my-db",
            "/airflow/connections/pg",
            "airflow/variables/env",
            "/airflow/variables/env",
            "secretsmanager:GetSecretValue",
            "ssm:GetParameter",
            compat_checks.PARAMETER_STORE_BACKEND,
            compat_checks.CONNECTIONS_DOCS,
        ]:
            self.assertIn(text, notice)
        self.assertIn("Connection ids my-db cannot be provided as AIRFLOW_CONN_*", notice)

    def test_deferrable(self):
        dag = SimpleNamespace(
            dag_id="d",
            tasks=[SimpleNamespace(task_id="a", deferrable=True), SimpleNamespace(task_id="b", deferrable=False)],
        )
        with patch.object(compat_checks, "print_warning") as warn:
            compat_checks.check_deferrable(dag)
        warn.assert_called_once()
        self.assertIn(": a", warn.call_args[0][0])

    def test_run_checks_never_raises(self):
        with patch.object(compat_checks, "print_warning") as warn:
            compat_checks.run_checks(object(), [123])
        self.assertIn("Skipped", warn.call_args[0][0])
