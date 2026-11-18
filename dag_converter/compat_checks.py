"""MWAA Serverless compatibility warnings. These only warn; they never fail a conversion."""

import ast
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from dag_converter.code_package import iter_strings
from dag_converter.console_config import print_warning

ENV_KEY_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
RESERVED_ENV_KEYS = {"PATH", "PYTHONPATH", "HOME", "AIRFLOW_HOME", "TMPDIR", "PYTHONUNBUFFERED", "LC_CTYPE"}
ALLOWED_AIRFLOW_KEYS = {"AIRFLOW__WORKERS__SECRETS_BACKEND", "AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS"}
# Connections the service provides
DEFAULT_CONNECTIONS = {"aws_default", "duckdb_default"}
SECRETS_MANAGER_BACKEND = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
PARAMETER_STORE_BACKEND = "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend"
CONNECTIONS_DOCS = "https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/connections.html"
JINJA_BLOCK = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)
JINJA_REFERENCE = re.compile(r"""\b(conn|var\.(?:value|json))(?:(?:\.get\(|\[)\s*['"]([^'"]+)['"]|\.(\w+))""")

_warned: set[str] = set()


def _warn_once(message: str) -> None:
    if message not in _warned:
        _warned.add(message)
        print_warning(message)


def env_key_problem(key: str) -> str | None:
    """Why the service would reject ``key`` as a workflow environment variable, or None."""
    if not ENV_KEY_PATTERN.fullmatch(key):
        return "is not a valid environment variable key (letters, digits and '_' only, starting with a letter)"
    if key.startswith("AWS_") or key in RESERVED_ENV_KEYS:
        return "is reserved by MWAA Serverless"
    if key.startswith("AIRFLOW__") and key not in ALLOWED_AIRFLOW_KEYS:
        return "is reserved by MWAA Serverless (AIRFLOW__* configuration)"
    return None


def _literal_env_keys(tree: ast.AST) -> Iterable[tuple[str, int]]:
    """Yield (key, line) for os.environ[...], os.environ.get(...), os.getenv(...) with literal keys."""
    for node in ast.walk(tree):
        key_node = None
        if isinstance(node, ast.Subscript) and _is_environ(node.value):
            key_node = node.slice
        elif isinstance(node, ast.Call) and node.args:
            func = node.func
            if (
                (isinstance(func, ast.Attribute) and func.attr == "get" and _is_environ(func.value))
                or (isinstance(func, ast.Attribute) and func.attr == "getenv")
                or (isinstance(func, ast.Name) and func.id == "getenv")
            ):
                key_node = node.args[0]
        if isinstance(key_node, ast.Constant) and isinstance(key_node.value, str):
            yield key_node.value, node.lineno


def _is_environ(node: ast.AST) -> bool:
    return (isinstance(node, ast.Attribute) and node.attr == "environ") or (
        isinstance(node, ast.Name) and node.id == "environ"
    )


def check_env_var_usage(py_files: Iterable[Path]) -> None:
    for py_file in py_files:
        try:
            tree = ast.parse(Path(py_file).read_text(), filename=str(py_file))
        except (SyntaxError, UnicodeDecodeError, OSError):
            continue
        for key, line in _literal_env_keys(tree):
            problem = env_key_problem(key)
            if problem:
                _warn_once(
                    f"{py_file}:{line} reads environment variable '{key}', which {problem}; it cannot be set "
                    "as a workflow environment variable on MWAA Serverless"
                )


def conn_ids_in(obj: Any) -> set[str]:
    """Values of keys ending in 'conn_id' anywhere in the converted Dag."""
    found = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).endswith("conn_id") and isinstance(value, str) and value:
                found.add(value)
            else:
                found |= conn_ids_in(value)
    elif isinstance(obj, list):
        for value in obj:
            found |= conn_ids_in(value)
    return found


def runtime_references(dag_dict: dict) -> tuple[set[str], set[str]]:
    """(connection ids, Variable keys) the converted Dag reads at run time: values of ``*conn_id`` keys and
    ``conn.X`` / ``conn['X']`` / ``conn.get('X')`` / ``var.value.X`` / ``var.json.get('X')`` ... references in Jinja
    templates."""
    conn_ids, var_keys = conn_ids_in(dag_dict), set()
    for _, _, value in iter_strings(dag_dict):
        for block in JINJA_BLOCK.findall(value):
            for kind, quoted, name in JINJA_REFERENCE.findall(block):
                if quoted or name != "get":
                    (conn_ids if kind == "conn" else var_keys).add(quoted or name)
    return conn_ids - DEFAULT_CONNECTIONS, var_keys


def print_runtime_notice(conn_ids: Iterable[str], var_keys: Iterable[str]) -> None:
    """One notice listing the connections and Variables the converted workflows need on MWAA Serverless."""
    conn_ids, var_keys = sorted(set(conn_ids) - DEFAULT_CONNECTIONS), sorted(set(var_keys))
    if not conn_ids and not var_keys:
        return
    lines = [
        "The converted workflows read these Airflow connections and Variables at run time. Before running them "
        "on MWAA Serverless, create them in AWS Secrets Manager, AWS Systems Manager Parameter Store, or your "
        "preferred secrets backend (names for the default prefixes):",
        *(
            f"  connection {conn_id}: secret airflow/connections/{conn_id} or parameter /airflow/connections/{conn_id}"
            for conn_id in conn_ids
        ),
        *(
            f"  Variable {key}: secret airflow/variables/{key} or parameter /airflow/variables/{key}"
            for key in var_keys
        ),
    ]
    invalid = [conn_id for conn_id in conn_ids if not ENV_KEY_PATTERN.fullmatch(f"AIRFLOW_CONN_{conn_id.upper()}")]
    if invalid:
        lines.append(
            "Connection ids " + ", ".join(invalid) + " cannot be provided as AIRFLOW_CONN_* environment variables "
            "(invalid key characters); use a secrets backend for them."
        )
    lines += [
        "The workflow execution role needs secretsmanager:GetSecretValue (or ssm:GetParameter) on them, and the "
        "workflow needs the secrets backend in its environment: AIRFLOW__WORKERS__SECRETS_BACKEND="
        f"{SECRETS_MANAGER_BACKEND} with AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS connections_prefix "
        "airflow/connections and variables_prefix airflow/variables (the create-workflow command printed with "
        f"--bucket includes it), or {PARAMETER_STORE_BACKEND} with /airflow/connections and /airflow/variables.",
        f"See {CONNECTIONS_DOCS}",
    ]
    print_warning("\n".join(lines), markup=False, soft_wrap=True)


def check_deferrable(dag_object: Any) -> None:
    tasks = [
        task.task_id
        for task in getattr(dag_object, "tasks", [])
        if getattr(task, "deferrable", False) is True or getattr(task, "start_from_trigger", False) is True
    ]
    if tasks:
        _warn_once(
            f"Dag '{dag_object.dag_id}' has deferrable tasks, which MWAA Serverless does not support: "
            + ", ".join(sorted(tasks))
        )


def run_checks(dag_object: Any, py_files: Iterable[Path]) -> None:
    try:
        check_env_var_usage(py_files)
        check_deferrable(dag_object)
    except Exception as e:  # warnings only: never fail a conversion
        print_warning(f"Skipped MWAA Serverless compatibility checks: {e}")
