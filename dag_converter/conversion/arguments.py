"""Checks for Dag and task arguments that DagFactory cannot represent."""

from typing import Any

from dag_converter.conversion.exceptions import InvalidDagParameter, InvalidOperatorParameter

# Dag arguments DagFactory 1.0 does not pass to the Dag, so they would be dropped
DAG_ARGUMENTS_IGNORED_BY_DAGFACTORY = {
    "fail_fast",
    "fail_stop",
    "jinja_environment_kwargs",
    "max_consecutive_failed_dag_runs",
    "owner_links",
    "template_undefined",
    "user_defined_filters",
    "user_defined_macros",
}


def get_single_callback(owner: str, key: str, value: Any, error: type[Exception]) -> Any:
    """DagFactory accepts a single callback per callback argument. Unwraps lists of one callback"""
    if key.endswith("_callback") and isinstance(value, list | tuple):
        if len(value) > 1:
            raise error(f"{owner} passes {len(value)} callables to '{key}'. DagFactory supports a single callback")
        return value[0] if value else None
    return value


def get_task_argument_value(task_id: str, key: str, value: Any) -> Any:
    """Check a task argument, returning the value to write"""
    return get_single_callback(f"Task '{task_id}'", key, value, InvalidOperatorParameter)


def get_dag_argument_value(key: str, value: Any) -> Any:
    """
    Check a Dag argument or default argument, returning the value to write.
    Returns None for empty arguments DagFactory ignores.
    """
    if key in DAG_ARGUMENTS_IGNORED_BY_DAGFACTORY:
        if not value:
            return None
        raise InvalidDagParameter(f"Dag argument '{key}' is not supported by DagFactory, which would drop it")
    return get_single_callback("Dag", key, value, InvalidDagParameter)
