"""Parse-time environment for loading Python Dags locally.

Sets up, for the duration of a conversion:
* AIRFLOW_HOME (defaults to the folder containing the Dag folder)
* a throwaway SQLite metadata database, unless one is configured
* a secrets backend (``parse_secrets``) that resolves --conn/--var overrides, wraps the user's
  backend and returns placeholder Connections for any conn_id that is not otherwise defined
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
from collections.abc import Iterable, Iterator
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
    """Overrides and bookkeeping shared with the secrets backend (same process)."""

    conns: dict[str, str] = {}
    variables: dict[str, str] = {}
    temp_db_url: str | None = None
    db_ready = False
    requested_conn_ids: set[str] = set()
    stubbed_conn_ids: set[str] = set()
    dag_folder: Path | None = None


state = _State


def find_dag_root(input_path: Path) -> Path:
    """The Dag folder root: ``--dag-folder`` if given, otherwise the nearest folder named ``dags`` among the
    input's folder (a file's parent) and its two parents, otherwise the input's folder."""
    if state.dag_folder:
        return state.dag_folder
    path = Path(input_path).absolute()
    start = path if path.is_dir() else path.parent
    for candidate in (start, *start.parents[:2]):
        if candidate.name == "dags":
            return candidate
    return start


def default_airflow_home(input_path: Path) -> Path:
    """Folder containing ``dags/`` if the input is inside a ``dags`` folder, otherwise the input's directory."""
    root = find_dag_root(input_path)
    return root.parent if root.name == "dags" else root


def parse_assignments(values: Iterable[str], option: str) -> dict[str, str]:
    """Parse repeated KEY=VALUE options."""
    result = {}
    for value in values or []:
        key, sep, rest = value.partition("=")
        if not sep or not key:
            raise ValueError(f"Invalid {option} value '{value}', expected KEY=VALUE")
        result[key] = rest
    return result


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
def parse_environment(
    input_path: Path,
    airflow_home: Path | None = None,
    conns: Iterable[str] = (),
    variables: Iterable[str] = (),
    dag_folder: Path | None = None,
) -> Iterator[None]:
    """Set up the parse-time environment; restores environment variables and sys.path on exit."""
    parsed_conns = parse_assignments(conns, "--conn")
    parsed_vars = parse_assignments(variables, "--var")
    state.dag_folder = Path(dag_folder).expanduser().absolute() if dag_folder else None
    new_env: dict[str, str] = {"PYTHONDONTWRITEBYTECODE": "1"}
    if airflow_home is not None:
        new_env["AIRFLOW_HOME"] = str(Path(airflow_home).expanduser().absolute())
    elif not os.environ.get("AIRFLOW_HOME"):
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

    state.conns, state.variables = parsed_conns, parsed_vars
    state.requested_conn_ids = set()
    state.stubbed_conn_ids = set()

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
        state.dag_folder = None
        if reconfigure:
            _configure_orm()
        shutil.rmtree(temp_dir, ignore_errors=True)
        if state.stubbed_conn_ids:
            print_warning(
                "Placeholder Connections were used for " + ", ".join(sorted(state.stubbed_conn_ids)) + ". Values "
                "derived from them at parse time (e.g. hostnames in templated strings) are placeholders in the "
                "YAML; pass --conn ID=URI for real values."
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
    if re.search(r"Variable \S+ does not exist", text) or "SUPERVISOR_COMMS" in text:
        hints.append(
            "Hint: the Dag reads an Airflow Variable (or Connection) at parse time that is not defined. "
            "Pass --var KEY=VALUE (or --conn ID=URI) to provide it for conversion."
        )
    return "".join(f"\n{hint}" for hint in hints)
