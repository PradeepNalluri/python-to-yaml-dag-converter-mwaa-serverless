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
        self.old_cwd = Path.cwd()

    def tearDown(self):
        os.chdir(self.old_cwd)
        self.temp_dir.cleanup()

    def test_dag_root_is_cwd_dags_when_it_contains_the_input(self):
        os.chdir(self.root / "project")
        for path in (self.dag_file, self.dag_file.parent):  # the input's form of the path is kept
            self.assertEqual(find_dag_root(path), self.root / "project" / "dags")
        for path in (Path("dags/sub/my_dag.py"), Path("dags")):
            self.assertEqual(find_dag_root(path), Path.cwd() / "dags")
        self.assertEqual(default_airflow_home(self.dag_file), self.root / "project")

    def test_dag_root_falls_back_to_input_folder(self):
        os.chdir(self.root / "project")  # has dags/, but the input is outside it
        self.assertEqual(find_dag_root(self.loose_file), self.root / "loose")
        self.assertEqual(find_dag_root(self.root / "loose"), self.root / "loose")
        os.chdir(self.root)  # no dags/ here
        self.assertEqual(find_dag_root(self.dag_file), self.dag_file.parent)
        self.assertEqual(find_dag_root(self.dag_file.parent), self.dag_file.parent)
        self.assertEqual(default_airflow_home(self.loose_file), Path.cwd())
        self.assertEqual(default_airflow_home(self.root / "project" / "dags"), self.root / "project")

    @patch.dict(os.environ, {}, clear=False)
    def test_parse_environment_defaults_and_restores(self):
        for key in ("AIRFLOW_HOME", parse_env.SQL_ALCHEMY_CONN_ENV, parse_env.SECRETS_BACKEND_ENV):
            os.environ.pop(key, None)
        os.chdir(self.root / "project")
        with parse_environment(self.dag_file):
            self.assertEqual(os.environ["AIRFLOW_HOME"], str(self.root / "project"))
            self.assertIn(str(self.root / "project" / "dags"), sys.path)
            self.assertTrue(os.environ[parse_env.SQL_ALCHEMY_CONN_ENV].startswith("sqlite:///"))
            self.assertNotIn(str(self.root), os.environ[parse_env.SQL_ALCHEMY_CONN_ENV])
            self.assertEqual(os.environ[parse_env.SECRETS_BACKEND_ENV], parse_env.SECRETS_BACKEND_PATH)
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
        os.environ["AIRFLOW_HOME"] = str(self.root)
        (self.root / "airflow.cfg").write_text(
            '[secrets]\nbackend = my.Backend\nbackend_kwargs = {"a": 1}\n[cosmos]\ncache_dir = /keep\n'
        )
        with parse_environment(self.dag_file):
            self.assertEqual(os.environ[parse_env.SECRETS_BACKEND_ENV], parse_env.SECRETS_BACKEND_PATH)
            self.assertEqual(os.environ[parse_env.WRAPPED_BACKEND_ENV], "my.Backend")
            self.assertEqual(os.environ[parse_env.WRAPPED_BACKEND_KWARGS_ENV], '{"a": 1}')
            self.assertNotIn(parse_env.COSMOS_CACHE_DIR_ENV, os.environ)

    @patch.dict(os.environ, {}, clear=False)
    def test_metadata_db_is_initialized_in_temp_dir(self):
        import sqlite3

        os.environ.pop(parse_env.SQL_ALCHEMY_CONN_ENV, None)
        os.environ["AIRFLOW_HOME"] = str(self.root / "project")
        with parse_environment(self.dag_file):
            parse_env.ensure_metadata_db()
            self.assertTrue(parse_env.state.db_ready)
            db_path = parse_env.state.temp_db_url.removeprefix("sqlite:///")
            tables = {row[0] for row in sqlite3.connect(db_path).execute("select name from sqlite_master")}
            self.assertIn("variable", tables)
        self.assertFalse(Path(db_path).exists())
        self.assertEqual(list((self.root / "project").iterdir()), [self.root / "project" / "dags"])


class FailingBackend:
    def __init__(self, **kwargs):
        raise RuntimeError("no credentials")


class TestSecretsBackend(unittest.TestCase):
    def setUp(self):
        parse_env.state.temp_db_url = "sqlite:////nonexistent"
        parse_env.state.db_ready = False
        self.backend = parse_secrets.ParseSecretsBackend()

    def tearDown(self):
        parse_env.state.temp_db_url = None

    def test_environment_connection(self):
        with patch.dict(os.environ, {"AIRFLOW_CONN_FROM_ENV": "mysql://u:p@envhost/db"}):
            conn = self.backend.get_connection("from_env")
        self.assertEqual(conn.host, "envhost")

    def test_environment_variable_and_missing_values(self):
        with patch.dict(os.environ, {"AIRFLOW_VAR_X": "real"}):
            self.assertEqual(self.backend.get_variable("x"), "real")
        self.assertIsNone(self.backend.get_variable("missing"))
        self.assertIsNone(self.backend.get_connection("missing"))

    def test_wrapped_backend_is_consulted_for_variables_and_config(self):
        class UserBackend:
            def get_variable(self, key):
                return {"from_user": "u"}.get(key)

            def get_config(self, key):
                raise RuntimeError("unavailable")

        self.backend.wrapped = UserBackend()
        with patch.dict(os.environ, {"AIRFLOW_VAR_FROM_USER": "env"}):
            self.assertEqual(self.backend.get_variable("from_user"), "u")
        self.assertIsNone(self.backend.get_config("x"))

    def test_wrapped_backend_that_fails_to_start_is_skipped(self):
        env = {
            parse_env.WRAPPED_BACKEND_ENV: f"{__name__}.FailingBackend",
            parse_env.WRAPPED_BACKEND_KWARGS_ENV: '{"region_name": "x"}',
            "AIRFLOW_VAR_FROM_ENV": "env",
        }
        with patch.dict(os.environ, env), patch.object(parse_secrets, "print_warning") as warn:
            backend = parse_secrets.ParseSecretsBackend()
            self.assertIsNone(backend.wrapped)
            self.assertEqual(backend.get_variable("from_env"), "env")
        self.assertIsNone(backend.get_variable("missing"))
        self.assertIsNone(backend.get_connection("missing"))
        self.assertIsNone(backend.get_config("x"))
        warn.assert_called_once()
        self.assertIn("FailingBackend", warn.call_args.args[0])
        self.assertIn("no credentials", warn.call_args.args[0])


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

    def test_no_hint(self):
        self.assertEqual(import_error_hints({"a.py": "SyntaxError"}), "")
