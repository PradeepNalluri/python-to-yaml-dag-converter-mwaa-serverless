import re
from typing import Any

from dag_converter.conversion.exceptions import InvalidOperatorError


def handle_python_decorator(
    task_dict: dict[str, Any],
    decorator_import_path: str,
    parameter_names: list[str],
    operator_task_ids: set[str] | None = None,
):
    """
    Handle special conversion for TaskFlow decorators

    Args:
        task_dict: Converted task, with python callables and XComArgs already cleaned up
        decorator_import_path: Import path of the decorator,
            e.g. airflow.providers.standard.decorators.python.python_task
        parameter_names: Parameter names of the decorated python callable, in positional order
        operator_task_ids: Ids of the Dag's operator (non-TaskFlow) tasks. When given, arguments are checked not
            to reference their output, which DagFactory cannot express
    """
    # Replace 'operator' with 'decorator', keeping it as the first key
    del task_dict["operator"]
    task_arguments = dict(task_dict)
    task_dict.clear()
    task_dict["decorator"] = decorator_import_path
    task_dict.update(task_arguments)

    # DagFactory does not support having the parameters within 'op_args' or 'op_kwargs'
    # Move the parameters within 'op_args' and 'op_kwargs' up one level
    move_call_arguments(task_dict, parameter_names)

    # Handle 'partial' during DTM specifically for TaskFlow
    # Move parameters up one level
    if "partial" in task_dict:
        move_call_arguments(task_dict["partial"], parameter_names)

    if operator_task_ids is not None:
        check_upstream_references(task_dict, operator_task_ids)


def check_upstream_references(task_dict: dict[str, Any], operator_task_ids: set[str]):
    """
    DagFactory resolves '+<task_id>' in decorator task arguments to the task it creates for <task_id>. That is
    the XComArg of a decorator task, but the operator itself for an operator task, which TaskFlow would receive
    as the argument value instead of its output. Operator outputs cannot be expressed in decorator tasks.
    """
    arguments = {k: v for k, v in task_dict.items() if k not in ("expand", "partial")}
    arguments.update(task_dict.get("partial") or {})
    arguments.update(task_dict.get("expand") or {})
    for name, value in arguments.items():
        if not isinstance(value, str):
            continue
        upstream = None
        if value.startswith("+"):
            upstream = value[1:]
        elif value.endswith(".output"):
            upstream = value.removesuffix(".output")
        if upstream in operator_task_ids:
            raise InvalidOperatorError(
                f"TaskFlow argument '{name}' uses the output of operator task '{upstream}', "
                "which DagFactory decorator tasks do not support"
            )


def move_call_arguments(arguments: dict[str, Any], parameter_names: list[str]):
    """Replace 'op_args' and 'op_kwargs' with the callable parameters they are passed to"""
    op_args = extract_op_args(arguments.pop("op_args", None) or [])
    op_kwargs = extract_op_kwargs(arguments.pop("op_kwargs", None) or {})

    # Iterate through positional arguments first
    for i, value in enumerate(op_args):
        arguments[parameter_names[i]] = value
    for k, v in op_kwargs.items():
        arguments[k] = v


def extract_op_args(op_args_str):
    """Clean up and return op_args"""
    result = []
    for arg in op_args_str:
        if "task_instance.xcom_pull" in str(arg):
            result.append(cleanup_xcom(str(arg)))
        else:
            result.append(arg)
    return result


def extract_op_kwargs(op_kwargs_str):
    """Clean up and return op_kwargs"""
    result = {}
    for arg, value in op_kwargs_str.items():
        if "task_instance.xcom_pull" in str(value):
            result[arg] = cleanup_xcom(str(value))
        else:
            result[arg] = value
    return result


def cleanup_xcom(xcom_obj_str):
    # Extract task_id
    task_id_match = re.search(r"task_ids=['\"]([^'\"]*)['\"]", xcom_obj_str)
    task_id = task_id_match.group(1) if task_id_match else None

    # Extract key
    key_match = re.search(r"key=['\"]([^'\"]*)['\"]", xcom_obj_str)
    key = key_match.group(1) if key_match else "return_value"  # Default to return_value if not specified

    if key == "return_value":
        return f"+{task_id}"

    return xcom_obj_str
