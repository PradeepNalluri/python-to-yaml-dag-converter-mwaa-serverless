"""Checks for task arguments that DagFactory cannot represent."""

from typing import Any

from dag_converter.conversion.exceptions import InvalidOperatorParameter

# Constructor arguments that describe the Dag structure rather than the task. Dag context managers inject
# these into every operator, and DagFactory rebuilds them from the YAML
STRUCTURAL_TASK_PARAMETERS = {"dag", "task_group"}

# Task arguments DagFactory imports from "<module>.<function>" strings, besides the on_*_callback arguments
CALLABLE_ARGUMENTS = {"python_callable", "execution_date_fn"}


def is_callable_argument(key: str) -> bool:
    return key in CALLABLE_ARGUMENTS or key.endswith("_callback")


def check_callables(owner: str, key: str, value: Any, error: type[Exception]):
    """
    Reject callables DagFactory would pass on as their "<module>.<function>" name instead of importing them:
    callables in other arguments, and callables nested in containers
    """
    if is_callable_argument(key):
        items = value if isinstance(value, list | tuple) else [value]
        nested = [v for item in items if not callable(item) for v in find_callables(item)]
    else:
        nested = list(find_callables(value))
    if nested:
        name = getattr(nested[0], "__qualname__", repr(nested[0]))
        raise error(
            f"{owner} passes the callable {name} in '{key}'. DagFactory only imports callables in "
            f"{', '.join(sorted(CALLABLE_ARGUMENTS))} and on_*_callback arguments, so it would receive a string"
        )


def find_callables(value: Any):
    """Callables in 'value', including in dicts, lists, tuples and sets"""
    if isinstance(value, str | bytes) or value is None:
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from find_callables(item)
    elif isinstance(value, list | tuple | set | frozenset):
        for item in value:
            yield from find_callables(item)
    elif callable(value):
        yield value


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
    check_callables(f"Task '{task_id}'", key, value, InvalidOperatorParameter)
    return get_single_callback(f"Task '{task_id}'", key, value, InvalidOperatorParameter)

