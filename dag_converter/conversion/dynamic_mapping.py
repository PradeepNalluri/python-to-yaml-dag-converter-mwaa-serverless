import re
from typing import Any

from airflow.models.mappedoperator import MappedOperator
from airflow.sdk.definitions._internal.expandinput import ListOfDictsExpandInput
from airflow.utils.types import ArgNotSet

from dag_converter.cleanup import get_cleanup_dag
from dag_converter.conversion.exceptions import InvalidOperatorError

# Constructor arguments that describe the Dag structure rather than the task
STRUCTURAL_PARTIAL_PARAMETERS = {"dag", "task_group"}


def handle_dynamic_task_mapping(
    tasks_dict: dict[str, Any],
    task_types: dict[str, str],
    task: MappedOperator,
    operator_defaults: dict[str, Any],
    dag_default_args: dict[str, Any],
):
    """
    Handle dynamic task mapping

    Args:
        tasks_dict: Converted task
        task_types: Task id to operator type of every task in the Dag
        task: Mapped task
        operator_defaults: Constructor defaults of the mapped operator class
        dag_default_args: Dag default_args, which Airflow merges into 'partial_kwargs'
    """
    if isinstance(getattr(task, "expand_input", None), ListOfDictsExpandInput):
        raise InvalidOperatorError(
            f"Task '{task.task_id}' uses expand_kwargs, which DagFactory does not support. Use expand instead"
        )

    if hasattr(task, "op_kwargs_expand_input"):
        tasks_dict["expand"] = get_cleanup_dag(task.op_kwargs_expand_input.value)
        reformat(tasks_dict["expand"], task_types)
    elif hasattr(task, "expand_input"):
        tasks_dict["expand"] = get_cleanup_dag(task.expand_input.value)
        reformat(tasks_dict["expand"], task_types)

        # Handle special case when 'op_kwargs' is in the 'expand' dictionary
        if "op_kwargs" in tasks_dict["expand"]:
            reformat(tasks_dict["expand"]["op_kwargs"], task_types)

    if hasattr(task, "partial_kwargs"):
        # Airflow fills 'partial_kwargs' with the BaseOperator defaults and the Dag default_args.
        # Keep only the values that differ from those, i.e. the arguments the user passed to partial()
        # The converted task gets the Dag default_args value, or the constructor default, for missing values.
        # TaskGroup default_args differ from those, so they are kept
        partial = {
            key: value
            for key, value in task.partial_kwargs.items()
            if key not in STRUCTURAL_PARTIAL_PARAMETERS
            and key not in tasks_dict
            and not is_default_value(value, dag_default_args.get(key, operator_defaults.get(key)))
        }
        tasks_dict["partial"] = get_cleanup_dag(partial)

        # Handle Python Operator that uses DTM
        # The 'python_callable' will be set to be a string representation of the actual function
        # TODO: need better way to create the python_callable
        if "python_callable" in tasks_dict["partial"]:
            tasks_dict["python_callable"] = tasks_dict["partial"].pop("python_callable")
        if not tasks_dict["partial"]:
            del tasks_dict["partial"]


def is_default_value(value: Any, default: Any) -> bool:
    """Check if 'value' is the effective default. Airflow stores some None defaults as empty containers"""
    if isinstance(value, ArgNotSet) or (value is None and default is None):
        return True
    if default is None and isinstance(value, list | tuple | dict | set) and not value:
        return True
    try:
        return bool(value == default)
    except Exception:
        return False


def reformat(expand_dict: dict | list | tuple, task_types: dict[str, str]):
    """Reformat XComs to turn unserializable dictionary format into just the task id"""
    if isinstance(expand_dict, dict):
        iterator = expand_dict.items()
    elif isinstance(expand_dict, list | tuple):
        iterator = enumerate(expand_dict)
    print(expand_dict)

    for k, v in iterator:
        # Handle expansion through task XComs
        if "task_instance.xcom_pull" in v:
            # TODO: this is an attempt at handling DTM while mixing TaskFlow and regular operators
            # Currently, this does not work, and DagFactory does not seem to support a solution
            task_id_match = re.search(r"task_ids=['\"]([^'\"]*)['\"]", v)
            task_id = str(task_id_match.group(1) if task_id_match else None)

            if task_types[task_id] == "_PythonDecoratedOperator":
                expand_dict[k] = f"+{task_id}"
            else:
                expand_dict[k] = f"{task_id}.output"
