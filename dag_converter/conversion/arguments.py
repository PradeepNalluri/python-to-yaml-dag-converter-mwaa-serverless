"""Checks for task arguments that DagFactory cannot represent."""

from typing import Any

from dag_converter.conversion.exceptions import InvalidOperatorParameter

# Constructor arguments that describe the Dag structure rather than the task. Dag context managers inject
# these into every operator, and DagFactory rebuilds them from the YAML
STRUCTURAL_TASK_PARAMETERS = {"dag", "task_group"}


def unwrap_callbacks(key: str, value: Any) -> Any:
    """Unwraps callback lists of at most one callback. Airflow stores callbacks of mapped tasks as lists"""
    if key.endswith("_callback") and isinstance(value, list | tuple) and len(value) <= 1:
        return value[0] if value else None
    return value


def get_single_callback(owner: str, key: str, value: Any, error: type[Exception]) -> Any:
    """DagFactory accepts a single callback per callback argument. Unwraps lists of one callback"""
    if key.endswith("_callback") and isinstance(value, list | tuple) and len(value) > 1:
        raise error(f"{owner} passes {len(value)} callables to '{key}'. DagFactory supports a single callback")
    return unwrap_callbacks(key, value)


def get_task_argument_value(task_id: str, key: str, value: Any) -> Any:
    """Check a task argument, returning the value to write"""
    return get_single_callback(f"Task '{task_id}'", key, value, InvalidOperatorParameter)
