"""Tests for parse_env module."""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dag_converter import parse_env, parse_secrets
from dag_converter.parse_env import (
    default_airflow_home,
    find_dag_root,
    import_error_hints,
    parse_assignments,
    parse_environment,
)


class TestPaths(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        (self.root / "project" / "dags" / "sub").mkdir(parents=True)
        self.dag_file = self.root / "project" / "dags" / "sub" / "my_dag.py"
        self.dag_file.write_text("")
        (self.root / "loose").mkdir()
        self.loose_file = self.root / "loose" / "dag.py"
        self.loose_file.write_text("")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_dag_root_inside_dags_folder(self):
        self.assertEqual(find_dag_root(self.dag_file), self.root / "project" / "dags")
        self.assertEqual(default_airflow_home(self.dag_file), self.root / "project")

    def test_dag_root_ignores_distant_dags_ancestor(self):
        deep = self.root / "project" / "dags" / "a" / "b" / "c" / "d.py"
        deep.parent.mkdir(parents=True)
        deep.write_text("")
        self.assertEqual(find_dag_root(deep), deep.parent)
        self.assertEqual(find_dag_root(deep.parent.parent), self.root / "project" / "dags")

    def test_dag_folder_option(self):
        with parse_environment(self.dag_file, dag_folder=self.root / "project"):
            self.assertEqual(find_dag_root(self.dag_file), self.root / "project")
            self.assertIn(str(self.root / "project"), sys.path)
        self.assertEqual(find_dag_root(self.dag_file), self.root / "project" / "dags")

    def test_dag_root_outside_dags_folder(self):
        self.assertEqual(find_dag_root(self.loose_file), self.root / "loose")
        self.assertEqual(default_airflow_home(self.loose_file), self.root / "loose")
        self.assertEqual(default_airflow_home(self.root / "loose"), self.root / "loose")

    def test_parse_assignments(self):
        self.assertEqual(
            parse_assignments(["a=b", "uri=x://u:p@h/db?a=1"], "--conn"), {"a": "b", "uri": "x://u:p@h/db?a=1"}
        )
        with self.assertRaises(ValueError):
            parse_assignments(["novalue"], "--var")

    @patch.dict(os.environ, {}, clear=False)
    def test_parse_environment_defaults_and_restores(self):
        for key in ("AIRFLOW_HOME", parse_env.SQL_ALCHEMY_CONN_ENV, parse_env.SECRETS_BACKEND_ENV):
            os.environ.pop(key, None)
        with parse_environment(self.dag_file, conns=["c=sqlite://"], variables=["k=v"]):
            self.assertEqual(os.environ["AIRFLOW_HOME"], str(self.root / "project"))
            self.assertTrue(os.environ[parse_env.SQL_ALCHEMY_CONN_ENV].startswith("sqlite:///"))
            self.assertNotIn(str(self.root), os.environ[parse_env.SQL_ALCHEMY_CONN_ENV])
            self.assertEqual(os.environ[parse_env.SECRETS_BACKEND_ENV], parse_env.SECRETS_BACKEND_PATH)
            self.assertEqual(parse_env.state.conns, {"c": "sqlite://"})
            self.assertEqual(parse_env.state.variables, {"k": "v"})
            cosmos_cache = os.environ[parse_env.COSMOS_CACHE_DIR_ENV]
            self.assertTrue(sys.dont_write_bytecode)
            self.assertEqual(os.environ["PYTHONDONTWRITEBYTECODE"], "1")
        self.assertFalse(Path(cosmos_cache).parent.exists())
        self.assertEqual(sys.dont_write_bytecode, False)
        self.assertNotIn(parse_env.COSMOS_CACHE_DIR_ENV, os.environ)
        self.assertNotIn("AIRFLOW_HOME", os.environ)
        self.assertNotIn(parse_env.SQL_ALCHEMY_CONN_ENV, os.environ)
        self.assertNotIn(parse_env.SECRETS_BACKEND_ENV, os.environ)

    @patch.dict(os.environ, {"AIRFLOW_HOME": "/explicit/home", parse_env.SQL_ALCHEMY_CONN_ENV: "postgresql://x"})
    def test_parse_environment_respects_user_settings(self):
        with parse_environment(self.dag_file):
            self.assertEqual(os.environ["AIRFLOW_HOME"], "/explicit/home")
            self.assertEqual(os.environ[parse_env.SQL_ALCHEMY_CONN_ENV], "postgresql://x")
            self.assertIsNone(parse_env.state.temp_db_url)

    @patch.dict(os.environ, {}, clear=False)
    def test_secrets_backend_from_airflow_cfg_is_wrapped(self):
        os.environ.pop(parse_env.SECRETS_BACKEND_ENV, None)
        (self.root / "airflow.cfg").write_text(
            '[secrets]\nbackend = my.Backend\nbackend_kwargs = {"a": 1}\n[cosmos]\ncache_dir = /keep\n'
        )
        with parse_environment(self.dag_file, airflow_home=self.root):
            self.assertEqual(os.environ[parse_env.SECRETS_BACKEND_ENV], parse_env.SECRETS_BACKEND_PATH)
            self.assertEqual(os.environ[parse_env.WRAPPED_BACKEND_ENV], "my.Backend")
            self.assertEqual(os.environ[parse_env.WRAPPED_BACKEND_KWARGS_ENV], '{"a": 1}')
            self.assertNotIn(parse_env.COSMOS_CACHE_DIR_ENV, os.environ)

    def test_placeholder_warning_on_exit(self):
        with patch.object(parse_env, "print_warning") as warn, parse_environment(self.dag_file):
            parse_env.state.stubbed_conn_ids.add("db")
        self.assertIn("--conn", warn.call_args[0][0])

    @patch.dict(os.environ, {"AIRFLOW_HOME": "/explicit/home"})
    def test_airflow_home_option_wins(self):
        with parse_environment(self.dag_file, airflow_home=self.root):
            self.assertEqual(os.environ["AIRFLOW_HOME"], str(self.root))
        self.assertEqual(os.environ["AIRFLOW_HOME"], "/explicit/home")

    @patch.dict(os.environ, {}, clear=False)
    def test_metadata_db_is_initialized_in_temp_dir(self):
        import sqlite3

        os.environ.pop(parse_env.SQL_ALCHEMY_CONN_ENV, None)
        with parse_environment(self.dag_file, airflow_home=self.root / "project"):
            parse_env.ensure_metadata_db()
            self.assertTrue(parse_env.state.db_ready)
            db_path = parse_env.state.temp_db_url.removeprefix("sqlite:///")
            tables = {row[0] for row in sqlite3.connect(db_path).execute("select name from sqlite_master")}
            self.assertIn("variable", tables)
        self.assertFalse(Path(db_path).exists())
        self.assertEqual(list((self.root / "project").iterdir()), [self.root / "project" / "dags"])


class TestSecretsBackend(unittest.TestCase):
    def setUp(self):
        parse_env.state.conns = {"given": "postgres://u:p@db:5432/x"}
        parse_env.state.variables = {"my_var": "value"}
        parse_env.state.temp_db_url = "sqlite:////nonexistent"
        parse_env.state.db_ready = False
        self.backend = parse_secrets.ParseSecretsBackend()

    def tearDown(self):
        parse_env.state.conns, parse_env.state.variables = {}, {}
        parse_env.state.temp_db_url = None

    def test_override_takes_precedence(self):
        with patch.dict(os.environ, {"AIRFLOW_CONN_GIVEN": "mysql://other"}):
            conn = self.backend.get_connection("given")
        self.assertEqual((conn.conn_type, conn.host), ("postgres", "db"))

    def test_environment_connection_used_before_placeholder(self):
        with patch.dict(os.environ, {"AIRFLOW_CONN_FROM_ENV": "mysql://u:p@envhost/db"}):
            conn = self.backend.get_connection("from_env")
        self.assertEqual(conn.host, "envhost")

    def test_placeholder_for_undefined_connection(self):
        conn = self.backend.get_connection("my-redshift-conn")
        self.assertEqual(conn.conn_id, "my-redshift-conn")
        self.assertEqual(conn.conn_type, "generic")
        self.assertEqual(conn.host, "placeholder")
        self.assertIn("my-redshift-conn", parse_env.state.stubbed_conn_ids)

    def test_variables_are_not_faked(self):
        self.assertEqual(self.backend.get_variable("my_var"), "value")
        self.assertIsNone(self.backend.get_variable("missing"))

    def test_wrapped_backend_is_consulted_for_variables_and_config(self):
        class UserBackend:
            def get_variable(self, key):
                return {"from_user": "u"}.get(key)

            def get_config(self, key):
                raise RuntimeError("unavailable")

        self.backend.wrapped = UserBackend()
        self.assertEqual(self.backend.get_variable("from_user"), "u")
        self.assertEqual(self.backend.get_variable("my_var"), "value")
        self.assertIsNone(self.backend.get_variable("missing"))
        self.assertIsNone(self.backend.get_config("x"))


class TestImportErrorHints(unittest.TestCase):
    def test_known_mismatch(self):
        hints = import_error_hints({"dag.py": "ModuleNotFoundError: No module named 'cosmos'"})
        self.assertIn("astronomer-cosmos", hints)
        self.assertIn("unrelated", hints)

    def test_provider_and_plain_module(self):
        hints = import_error_hints(
            {"a.py": "No module named 'airflow.providers.google'", "b.py": "No module named 'yaml'"}
        )
        self.assertIn("apache-airflow-providers-google", hints)
        self.assertIn(" pyyaml'", hints)

    def test_hint_uses_uv_without_pip(self):
        error = {"b.py": "No module named 'yaml'"}
        with patch.object(parse_env.importlib.util, "find_spec", return_value=None):
            self.assertIn(f"uv pip install --python {sys.executable} pyyaml", import_error_hints(error))
        with patch.object(parse_env.importlib.util, "find_spec", return_value=object()):
            self.assertIn(f"{sys.executable} -m pip install pyyaml", import_error_hints(error))

    def test_missing_variable(self):
        self.assertIn("--var", import_error_hints({"a.py": "KeyError: 'Variable foo does not exist'"}))

    def test_no_hint(self):
        self.assertEqual(import_error_hints({"a.py": "SyntaxError"}), "")
