"""Tests for run-time Variable and Connection lookups (runtime_lookups.patched and RuntimeLookups)."""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from airflow.exceptions import AirflowNotFoundException
from airflow.sdk.exceptions import AirflowRuntimeError
from airflow.secrets.environment_variables import EnvironmentVariablesBackend

from dag_converter import compat_checks, runtime_lookups
from dag_converter.conversion_manager import ConversionManager
from dag_converter.dagbag import get_dag_object
from dag_converter.runtime_lookups import NO_DEFAULT, Lookup


def render(template: str, variables: dict, connections: dict) -> str:
    """Render with Airflow's task template environment and the context's var/conn accessors."""
    from airflow.sdk.definitions._internal.templater import SandboxedEnvironment
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.exceptions import ErrorType
    from airflow.sdk.execution_time import context
    from airflow.sdk.execution_time.comms import ErrorResponse

    def get_variable(key, deserialize_json):
        if key not in variables:
            raise AirflowRuntimeError(ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND))
        return variables[key]

    def get_connection(conn_id):
        if conn_id not in connections:
            raise AirflowRuntimeError(ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND))
        return Connection(conn_id=conn_id, conn_type="postgres", **connections[conn_id])

    with (
        patch.object(context, "_get_variable", get_variable),
        patch.object(context, "_get_connection", get_connection),
    ):
        env = SandboxedEnvironment()
        accessors = {
            "var": {"value": context.VariableAccessor(deserialize_json=False)},
            "conn": context.ConnectionAccessor(),
        }
        return env.from_string(template).render(**accessors)


class TestJinja(unittest.TestCase):
    def test_forms(self):
        self.assertEqual(Lookup("var", "env", default="dev").jinja(), "{{ var.value.get('env', 'dev') }}")
        self.assertEqual(Lookup("var", "env").jinja(), "{{ var.value.env }}")
        self.assertEqual(Lookup("var", "my-key").jinja(), "{{ var.value['my-key'] }}")
        self.assertEqual(Lookup("var", "get").jinja(), "{{ var.value.get('get') }}")
        self.assertEqual(Lookup("var", "n", default=None).jinja(), "{{ var.value.get('n', None) }}")
        self.assertIsNone(Lookup("var", "obj", default=object()).jinja())
        self.assertEqual(Lookup("conn", "pg", "host").jinja(), "{{ conn.pg.host }}")
        self.assertEqual(Lookup("conn", "my-db", "password").jinja(), "{{ conn['my-db'].password }}")

    def test_rendered_by_airflow(self):
        variables, connections = {"env": "prod", "my-key": "k"}, {"pg": {"host": "pghost"}, "my-db": {"login": "u"}}
        cases = [
            (Lookup("var", "env", default="dev"), "prod"),
            (Lookup("var", "missing", default="dev"), "dev"),
            (Lookup("var", "env"), "prod"),
            (Lookup("var", "my-key"), "k"),
            (Lookup("conn", "pg", "host"), "pghost"),
            (Lookup("conn", "my-db", "login"), "u"),
        ]
        for lookup, expected in cases:
            self.assertEqual(render(f"x {lookup.jinja()}", variables, connections), f"x {expected}")
        # Like Variable.get and get_connection in the Dag, a missing key without a default fails the task
        for lookup in (Lookup("var", "missing"), Lookup("var", "miss-ing"), Lookup("conn", "miss-ing", "host")):
            with self.assertRaises((AirflowRuntimeError, AirflowNotFoundException)):
                render(lookup.jinja(), variables, connections)

    def test_concrete_value(self):
        self.assertEqual(Lookup("var", "k", default="d", local="real").concrete(), "real")
        self.assertEqual(Lookup("var", "k", default="d").concrete(), "d")
        self.assertEqual(Lookup("var", "k", default=NO_DEFAULT).concrete(), "placeholder")
        self.assertEqual(Lookup("conn", "c", "password", local="secret").shown(), "***")


DAG_SOURCE = """
from datetime import datetime
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator

with DAG("runtime_dag", start_date=datetime(2024, 1, 1), schedule=None, tags=[Variable.get("tag", "t")]) as dag:
    BashOperator(
        task_id="deploy",
        bash_command=f"deploy --env {Variable.get('env', default_var='dev')} --key {Variable.get('api_key')}",
        retries=int(Variable.get("retries", default_var="2")),
        owner=Variable.get("owner", default_var="me"),
    )
    BashOperator(task_id="db", bash_command=f"psql -h {BaseHook.get_connection('my-db').host} "
                 f"-U {BaseHook.get_connection('pg').login}")
    BashOperator(task_id="cfg", bash_command=f"echo {Variable.get('cfg', deserialize_json=True)['x']}")
    if Variable.get("mode", default_var="a").upper() == "B":
        BashOperator(task_id="extra", bash_command="echo b")
"""


class TestRuntimeLookups(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.dag_file = self.temp_dir / "runtime_dag.py"
        self.dag_file.write_text(DAG_SOURCE)
        self.output = self.temp_dir / "out"
        backends = [EnvironmentVariablesBackend()]
        self.patches = [
            patch("airflow.models.variable.ensure_secrets_loaded", return_value=backends),
            patch("airflow.models.connection.ensure_secrets_loaded", return_value=backends),
            patch.dict(os.environ, {"AIRFLOW_VAR_CFG": '{"x": 1}', "AIRFLOW_CONN_PG": "postgres://user:pw@pghost/db"}),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        shutil.rmtree(self.temp_dir)

    def test_conversion(self):
        warnings = []
        with (
            patch.object(runtime_lookups, "print_warning", side_effect=lambda m, **_: warnings.append(m)),
            patch.object(compat_checks, "print_warning", side_effect=lambda m, **_: warnings.append(m)),
            patch("dag_converter.conversion_manager.get_dag_object", wraps=get_dag_object) as parse,
            patch("dag_converter.conversion_manager.boto3"),
            patch("dag_converter.conversion_manager.upload_package") as upload,
        ):
            ConversionManager(s3_bucket="bucket").start_conversion_process(self.dag_file, self.output)
        # --bucket: the create-workflow command gets the secrets backend environment
        self.assertIn("AIRFLOW__WORKERS__SECRETS_BACKEND", upload.call_args.args[5]["Variables"])
        text = (self.output / "runtime_dag.yaml").read_text()
        self.assertNotIn("__dagconv", text.lower())
        tasks = yaml.safe_load(text)["runtime_dag"]["tasks"]
        self.assertEqual(
            tasks["deploy"]["bash_command"],
            "deploy --env {{ var.value.get('env', 'dev') }} --key {{ var.value.api_key }}",
        )
        self.assertEqual(tasks["db"]["bash_command"], "psql -h {{ conn['my-db'].host }} -U {{ conn.pg.login }}")
        # Not templated, or used in Dag logic: the concrete value
        self.assertEqual(tasks["deploy"]["retries"], 2)
        self.assertEqual(tasks["deploy"]["owner"], "me")
        self.assertEqual(tasks["cfg"]["bash_command"], "echo 1")
        self.assertNotIn("extra", tasks)
        self.assertEqual(yaml.safe_load(text)["runtime_dag"]["tags"], ["t"])
        self.assertEqual(parse.call_count, 2)  # mode was upper-cased: parsed again with its concrete value

        fixed, not_templated, notice = warnings
        for text in ("Variable 'mode'", "Variable 'retries'", "Variable 'cfg': read with deserialize_json=True"):
            self.assertIn(text, fixed)
        self.assertNotIn("'env'", fixed)
        self.assertIn("deploy.owner: Variable 'owner' = 'me'", not_templated)
        self.assertIn("tags: Variable 'tag' = 't'", not_templated)
        for name in ("my-db", "pg", "api_key", "cfg", "env", "mode", "owner", "retries", "tag"):
            self.assertIn(f" {name}: ", notice)

    def test_cosmos_cache_variable_is_not_a_marker(self):
        from airflow.models import Variable

        with runtime_lookups.patched() as lookups:
            self.assertIsNone(Variable.get("cosmos_cache__dag", default_var=None))
            self.assertTrue(Variable.get("other").startswith("__dagconv_var_"))
        self.assertEqual(lookups.var_keys, {"other"})


class TestNoMarkers(unittest.TestCase):
    def test_assert_no_markers(self):
        runtime_lookups.assert_no_markers("bash_command: echo {{ var.value.x }}")
        with self.assertRaises(RuntimeError):
            runtime_lookups.assert_no_markers("bash_command: echo __DAGCONV_VAR_0__")


if __name__ == "__main__":
    unittest.main()
