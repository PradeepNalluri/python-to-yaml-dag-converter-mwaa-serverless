"""Parse-time environment for loading Python Dags locally.

Sets up, for the duration of a conversion:
* AIRFLOW_HOME, unless set (the folder containing ``dags/``, otherwise the current directory)
* a throwaway SQLite metadata database, unless one is configured
* a secrets backend (``parse_secrets``) that wraps the user's backend and returns placeholder
  Connections and Variables for anything that is not otherwise defined
* a throwaway Cosmos cache dir, and no ``__pycache__`` written into the project

Environment variables are set before Airflow is imported (the CLI imports Airflow lazily),
so Airflow's configuration picks them up.
"""

import atexit
import configparser
import contextlib
import importlib.util
import logging
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

from dag_converter.console_config import print_info, print_warning

SQL_ALCHEMY_CONN_ENV = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
SECRETS_BACKEND_ENV = "AIRFLOW__SECRETS__BACKEND"
SECRETS_BACKEND_KWARGS_ENV = "AIRFLOW__SECRETS__BACKEND_KWARGS"
SECRETS_BACKEND_PATH = "dag_converter.parse_secrets.ParseSecretsBackend"
WRAPPED_BACKEND_ENV = "_DAG_CONVERTER_WRAPPED_SECRETS_BACKEND"
WRAPPED_BACKEND_KWARGS_ENV = "_DAG_CONVERTER_WRAPPED_SECRETS_BACKEND_KWARGS"
COSMOS_CACHE_DIR_ENV = "AIRFLOW__COSMOS__CACHE_DIR"

# Module names that differ from the pip package that provides them
PIP_PACKAGE_NAMES = {
    "cosmos": "astronomer-cosmos",
    "yaml": "pyyaml",
    "dateutil": "python-dateutil",
    "dotenv": "python-dotenv",
    "sklearn": "scikit-learn",
    "PIL": "pillow",
    "bs4": "beautifulsoup4",
    "cv2": "opencv-python",
}


class _State:
    """Bookkeeping shared with the secrets backend (same process)."""

    temp_db_url: str | None = None
    db_ready = False
    requested_conn_ids: set[str] = set()
    stubbed_conn_ids: set[str] = set()
    stubbed_var_keys: set[str] = set()


state = _State


def find_dag_root(input_path: Path) -> Path:
    """The Dag folder root (``/usr/local/airflow/dags`` on the service): ``./dags`` if it contains the input,
    otherwise the input's folder (a file's parent)."""
    path = Path(input_path).absolute()
    cwd_dags = (Path.cwd() / "dags").resolve()
    for candidate in (path, *path.parents):
        if candidate.resolve() == cwd_dags:  # keep the input's form of the path (e.g. /tmp vs /private/tmp)
            return candidate
    return path if path.is_dir() else path.parent


def default_airflow_home(input_path: Path) -> Path:
    """The folder containing the Dag folder if it is named ``dags``, otherwise the current directory."""
    root = find_dag_root(input_path)
    return root.parent if root.name == "dags" else Path.cwd()


def _airflow_cfg() -> configparser.ConfigParser:
    """The user's airflow.cfg, read without importing Airflow (empty if missing or invalid)."""
    config_file = os.environ.get("AIRFLOW_CONFIG") or str(
        Path(os.environ.get("AIRFLOW_HOME", "~/airflow")).expanduser() / "airflow.cfg"
    )
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(config_file)
    except configparser.Error:
        return configparser.ConfigParser(interpolation=None)
    return parser


def _db_configured_by_user(cfg: configparser.ConfigParser) -> bool:
    if any(os.environ.get(SQL_ALCHEMY_CONN_ENV + suffix) for suffix in ("", "_CMD", "_SECRET")):
        return True
    return any(cfg.has_option("database", key) for key in ("sql_alchemy_conn", "sql_alchemy_conn_cmd"))


@contextlib.contextmanager
def parse_environment(input_path: Path) -> Iterator[None]:
    """Set up the parse-time environment; restores environment variables and sys.path on exit."""
    new_env: dict[str, str] = {"PYTHONDONTWRITEBYTECODE": "1"}
    if not os.environ.get("AIRFLOW_HOME"):
        new_env["AIRFLOW_HOME"] = str(default_airflow_home(input_path))

    old_env = {
        key: os.environ.get(key)
        for key in (
            *new_env,
            SQL_ALCHEMY_CONN_ENV,
            SECRETS_BACKEND_ENV,
            SECRETS_BACKEND_KWARGS_ENV,
            WRAPPED_BACKEND_ENV,
            WRAPPED_BACKEND_KWARGS_ENV,
            COSMOS_CACHE_DIR_ENV,
        )
    }
    os.environ.update(new_env)
    old_dont_write_bytecode, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    cfg = _airflow_cfg()
    temp_dir = tempfile.mkdtemp(prefix="dag-converter-")
    atexit.register(shutil.rmtree, temp_dir, ignore_errors=True)

    if not _db_configured_by_user(cfg):
        state.temp_db_url = f"sqlite:///{Path(temp_dir) / 'airflow.db'}"
        os.environ[SQL_ALCHEMY_CONN_ENV] = state.temp_db_url
    # Cosmos writes rendered profiles.yml (with credentials) and dbt artifacts to its cache dir
    if not os.environ.get(COSMOS_CACHE_DIR_ENV) and not cfg.has_option("cosmos", "cache_dir"):
        os.environ[COSMOS_CACHE_DIR_ENV] = str(Path(temp_dir) / "cosmos")

    # Install the parse-time secrets backend, wrapping a user-configured one (environment or airflow.cfg)
    user_backend = os.environ.get(SECRETS_BACKEND_ENV) or cfg.get("secrets", "backend", fallback="")
    if user_backend and user_backend != SECRETS_BACKEND_PATH:
        os.environ[WRAPPED_BACKEND_ENV] = user_backend
        os.environ[WRAPPED_BACKEND_KWARGS_ENV] = (
            os.environ.get(SECRETS_BACKEND_KWARGS_ENV) or cfg.get("secrets", "backend_kwargs", fallback="") or "{}"
        )
    os.environ[SECRETS_BACKEND_ENV] = SECRETS_BACKEND_PATH
    os.environ[SECRETS_BACKEND_KWARGS_ENV] = "{}"

    state.requested_conn_ids = set()
    state.stubbed_conn_ids = set()
    state.stubbed_var_keys = set()

    # Mirror the service, where the Dag folder root is on the import path
    dag_root = str(find_dag_root(input_path))
    old_sys_path = list(sys.path)
    if dag_root not in sys.path:
        sys.path.append(dag_root)

    print_info(f"AIRFLOW_HOME: {os.environ.get('AIRFLOW_HOME')}")
    try:
        yield
    finally:
        reconfigure = state.db_ready and "airflow.settings" in sys.modules
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        sys.path[:] = old_sys_path
        sys.dont_write_bytecode = old_dont_write_bytecode
        state.temp_db_url = None
        state.db_ready = False
        if reconfigure:
            _configure_orm()
        shutil.rmtree(temp_dir, ignore_errors=True)
        if state.stubbed_conn_ids:
            print_warning(
                "Placeholder Connections were used for " + ", ".join(sorted(state.stubbed_conn_ids)) + ". Values "
                "derived from them at parse time (e.g. hostnames in templated strings) are placeholders in the "
                "YAML. Set AIRFLOW_CONN_<ID> or use a secrets backend for real values."
            )
        if state.stubbed_var_keys:
            keys = sorted(state.stubbed_var_keys)
            print_warning(
                "Placeholder Variables were used for " + ", ".join(keys) + ". If the Dag's structure or task "
                "values depend on them, the YAML contains the placeholder. Read them with a Jinja template "
                f"(e.g. '{{{{ var.value.{keys[0]} }}}}') in the Dag, or set AIRFLOW_VAR_<KEY> to the real value "
                "before converting."
            )


def _configure_orm() -> None:
    from airflow import settings

    settings.configure_vars()
    settings.dispose_orm()
    settings.configure_orm()


def ensure_metadata_db() -> None:
    """Create the throwaway metadata DB schema. No-op unless parse_environment set up a temp DB."""
    if not state.temp_db_url or state.db_ready:
        return
    from airflow import settings

    if state.temp_db_url != settings.SQL_ALCHEMY_CONN:
        # Airflow was imported before the environment was set up; point the ORM at the temp DB
        _configure_orm()

    from airflow.utils import db

    quiet = ["alembic", "airflow.utils.db", "airflow.models.crypto"]
    levels = {name: logging.getLogger(name).level for name in quiet}
    for name in quiet:
        logging.getLogger(name).setLevel(logging.WARNING)
    try:
        db.initdb()
        state.db_ready = True
    except Exception as e:
        print_warning(f"Could not initialize the temporary metadata database ({e}); continuing without it")
    finally:
        for name, level in levels.items():
            logging.getLogger(name).setLevel(level)


def import_error_hints(import_errors: dict) -> str:
    """Return actionable hints for common DagBag import errors (empty string if none apply)."""
    hints = []
    text = "\n".join(str(err) for err in import_errors.values())
    for module in dict.fromkeys(re.findall(r"No module named '([^']+)'", text)):
        top = module.split(".")[0]
        if top == "airflow" and module.startswith("airflow.providers."):
            package = "apache-airflow-providers-" + module.split(".")[2].replace("_", "-")
        else:
            package = PIP_PACKAGE_NAMES.get(top, top)
        install = (
            f"{sys.executable} -m pip install {package}"
            if importlib.util.find_spec("pip")
            else f"uv pip install --python {sys.executable} {package}"
        )
        hint = (
            f"Hint: module '{module}' is not installed in the interpreter running dag-converter "
            f"({sys.executable}). Install it with '{install}' or pass --requirements <requirements.txt>."
        )
        if top == "cosmos":
            hint += " Note: the PyPI package 'cosmos' is unrelated to Astronomer Cosmos."
        hints.append(hint)
    return "".join(f"\n{hint}" for hint in hints)
