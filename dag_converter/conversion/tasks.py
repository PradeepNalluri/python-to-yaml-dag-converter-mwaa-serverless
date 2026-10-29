import importlib
import inspect
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.models.dag import DAG
from airflow.models.operator import Operator
from airflow.providers.standard.decorators.python import _PythonDecoratedOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.utils.types import NOTSET, ArgNotSet

from dag_converter.cleanup import is_dag_file_module
from dag_converter.console_config import print_warning
from dag_converter.conversion.arguments import get_task_argument_value
from dag_converter.conversion.dynamic_mapping import handle_dynamic_task_mapping
from dag_converter.conversion.exceptions import InvalidOperatorError, InvalidOperatorParameter
from dag_converter.dagbag import (
    USER_KWARGS_ATTRIBUTE,
    get_assigned_attributes,
    get_constructor_parameter_names,
    get_init_kwargs,
    values_differ,
    was_created_in_dag,
)
from dag_converter.schema_parser import ArgumentValidator
from dag_converter.taskflow_parser import TaskFlowAnalyzer

# Constructor arguments that describe the Dag structure rather than the task. Dag context managers inject
# these into every operator, and DagFactory rebuilds them from the YAML
STRUCTURAL_TASK_PARAMETERS = {"dag", "task_group"}

# Arguments Airflow sets from the Dag when a task is added to it
DATE_ARGUMENTS = {"start_date", "end_date"}

# Operators DagFactory creates only with a 'python_callable' from the YAML
PYTHON_CALLABLE_OPERATORS = (PythonOperator, BranchPythonOperator, PythonSensor)


def convert_tasks(
    taskflow_parser: TaskFlowAnalyzer, dag_object: DAG, dag_file_path: Path, validator: ArgumentValidator
):
    """Convert each task into acceptable DagFactory format"""
    tasks = {}
    task_types = {}
    
    # Get DAG-level params to filter them out from tasks
    dag_params_obj = getattr(dag_object, "params", None)
    dag_params = {}
    if dag_params_obj and hasattr(dag_params_obj, "items"):
        for key, param in dag_params_obj.items():
            if hasattr(param, "value"):
                dag_params[key] = param.value
            else:
                dag_params[key] = param

    dag_default_args = getattr(dag_object, "default_args", None)
    if not isinstance(dag_default_args, dict):
        dag_default_args = {}

    # First iteration through tasks to map each task id to its corresponding operator type
    for task in dag_object.tasks:
        task_id = task.task_id
        if not task_id:
            continue

        task_types[task_id] = task.task_type

    # Second iteration through tasks to handle the actual conversion
    for task in dag_object.tasks:
        task_id = task.task_id
        if not task_id:
            continue

        # Check if params was explicitly set (different from DAG params)
        init_kwargs = getattr(task, "_BaseOperator__init_kwargs", {})
        # Arguments the Dag file passed, recorded when the Dag is loaded by get_dag_object. Fall back to
        # __init_kwargs, which also holds arguments subclasses pass to their base class
        user_kwargs = getattr(task, "__dict__", {}).get(USER_KWARGS_ATTRIBUTE, init_kwargs)
        params_in_init = init_kwargs.get("params", {})
        params_explicitly_set = "params" in init_kwargs and params_in_init != dag_params

        # Edge case for DTM, since it stores the actual operator module in '_task_module' instead
        if isinstance(task, MappedOperator):
            if task.operator_class is _PythonDecoratedOperator:
                raise InvalidOperatorError("Operator PythonDecoratedOperator is not supported")

            operator_module = task._task_module
        else:
            operator_module = task.__module__
        full_operator_name = f"{operator_module}.{task.task_type}"

        # DagBag imports the Dag file under an internal module name that cannot be imported anywhere else
        if is_dag_file_module(operator_module):
            raise InvalidOperatorError(
                f"Operator {task.task_type} is defined in the Dag file. "
                "Move it to a module that can be imported where the YAML Dag runs"
            )
        warn_if_unverified_operator(task.task_type, full_operator_name, validator)

        # DagFactory requires 'python_callable' in the YAML for these operators, even for subclasses that set it
        if (
            not isinstance(task, MappedOperator)
            and isinstance(task, PYTHON_CALLABLE_OPERATORS)
            and "python_callable" not in user_kwargs
        ):
            raise InvalidOperatorError(
                f"Operator {task.task_type} sets python_callable itself. DagFactory requires python_callable "
                "for PythonOperator, BranchPythonOperator and PythonSensor subclasses"
            )

        tasks[task_id] = {"operator": full_operator_name}

        # Handle TaskFlow
        if isinstance(task, _PythonDecoratedOperator):
            raise InvalidOperatorError("Operator PythonDecoratedOperator is not supported")

        # Only the arguments the user passed are written. DagFactory runs the operator's __init__ again, so
        # values __init__ derived (attributes) or passed to its base class must not be written back.
        # Explicit None is written, it overrides default_args
        for key, value in user_kwargs.items():
            if key in STRUCTURAL_TASK_PARAMETERS or isinstance(value, ArgNotSet):
                continue
            # Airflow merges Dag default_args into every operator's kwargs. Those are already in the
            # Dag's default_args, so they are only repeated on the task when overridden
            if key in dag_default_args and not values_differ(value, dag_default_args[key]):
                continue
            # Skip params only if they match DAG params AND were not explicitly set
            if key == "params" and value == dag_params and not params_explicitly_set:
                continue
            tasks[task_id][key] = get_task_argument_value(task_id, key, value)

        if not isinstance(task, MappedOperator):
            for key, value in get_inherited_arguments(task, user_kwargs, dag_default_args).items():
                tasks[task_id][key] = get_task_argument_value(task_id, key, value)

            # Attributes the Dag file assigned after creating the operator
            for key, value in get_converted_assigned_attributes(task, user_kwargs, get_init_kwargs(task)).items():
                tasks[task_id][key] = get_task_argument_value(task_id, key, value)

        tasks[task_id]["dependencies"] = list(getattr(task, "upstream_task_ids", []))

        # Handle DTM
        if isinstance(task, MappedOperator):
            handle_dynamic_task_mapping(
                tasks[task_id],
                task_types,
                task,
                get_operator_defaults(task.operator_class),
                dag_default_args,
            )
            for key, value in tasks[task_id].get("partial", {}).items():
                tasks[task_id]["partial"][key] = get_task_argument_value(task_id, key, value)

        # Handle operator-specific parameters that we need to delete
        if "operators.appflow" in full_operator_name and "flow_update" in tasks[task_id]:
            del tasks[task_id]["flow_update"]

        if "sensors.eks" in full_operator_name and "target_state_type" in tasks[task_id]:
            del tasks[task_id]["target_state_type"]

        if "sensors.sagemaker" in full_operator_name and "resource_type" in tasks[task_id]:
            del tasks[task_id]["resource_type"]

        filter_config = [
            "SageMakerStartPipelineOperator",
            "SageMakerStopPipelineOperator",
            "SageMakerCreateExperimentOperator",
        ]
        if task.task_type in filter_config and "config" in tasks[task_id]:
            del tasks[task_id]["config"]

        if task.task_type == "MwaaDagRunSensor":
            if "success_states" in tasks[task_id]:
                del tasks[task_id]["success_states"]
            if "failure_states" in tasks[task_id]:
                del tasks[task_id]["failure_states"]

    return tasks


def get_inherited_arguments(task: Operator, user_kwargs: dict[str, Any], dag_default_args: dict[str, Any]):
    """
    Arguments the task inherited from other sources than the Dag default_args, which the YAML would not apply:
    TaskGroup default_args (merged with parent groups), and for operators added to the Dag after their creation,
    the values Airflow used instead of the Dag default_args
    """
    parameters = get_constructor_parameter_names(type(task))
    inherited = {}
    if not was_created_in_dag(task):
        # Airflow applied no default_args, DagFactory will. Keep the values the operator had
        for key in dag_default_args:
            if key in parameters and key not in user_kwargs and key not in DATE_ARGUMENTS:
                inherited[key] = getattr(task, key, None)
        return inherited

    group_default_args = getattr(getattr(task, "task_group", None), "default_args", None)
    if not isinstance(group_default_args, dict):
        return inherited
    for key, value in group_default_args.items():
        if key not in parameters or key in user_kwargs or key in STRUCTURAL_TASK_PARAMETERS:
            continue
        if key in dag_default_args and not values_differ(value, dag_default_args[key]):
            continue
        inherited[key] = value
    return inherited


def get_converted_assigned_attributes(
    task: Operator, user_kwargs: dict[str, Any], init_kwargs: dict[str, Any]
) -> dict[str, Any]:
    """
    Attributes the Dag file assigned after creating the operator, as task arguments.

    An assignment can be converted when the attribute holds the argument of the same name as passed, so
    DagFactory passing the new value to __init__ gives the same attribute
    """
    parameters = get_constructor_parameter_names(type(task))
    defaults = get_operator_defaults(type(task))
    converted = {}
    for key, (init_value, value) in get_assigned_attributes(task).items():
        if key in STRUCTURAL_TASK_PARAMETERS or isinstance(value, ArgNotSet):
            continue
        if key not in parameters and key not in init_kwargs and reads_keyword_argument(task, user_kwargs, key, value):
            converted[key] = value
            continue
        if key not in parameters and key not in init_kwargs:
            # The operator does not define the attribute, so it does not read it
            if isinstance(init_value, ArgNotSet):
                print_warning(
                    f"Task '{task.task_id}' assigns '{key}' after creating the operator. {task.task_type} does not "
                    "define it, ignoring it"
                )
                continue
            raise InvalidOperatorParameter(
                f"Task '{task.task_id}' assigns '{key}' after creating the operator. The converter cannot tell "
                f"whether {task.task_type} accepts it as an argument, so the assignment cannot be converted. "
                f"Pass {key}=... to the operator instead"
            )
        # The value __init__ received, including default_args and arguments of subclasses
        passed = init_kwargs[key] if key in init_kwargs else defaults.get(key, init_value)
        if isinstance(init_value, timedelta) and isinstance(passed, int | float) and not isinstance(passed, bool):
            passed = timedelta(seconds=passed)
        # __init__ normalizing None to an empty container is not a transformation
        normalized_none = passed is None and isinstance(init_value, list | dict | set | tuple) and not init_value
        if not normalized_none and values_differ(init_value, passed):
            raise InvalidOperatorParameter(
                f"Task '{task.task_id}' assigns '{key}' after creating the operator, but {task.task_type}.__init__ "
                "transforms that argument, so the assignment cannot be converted. Pass the value to the operator"
            )
        converted[key] = value
    return converted


def reads_keyword_argument(task: Operator, user_kwargs: dict[str, Any], key: str, value: Any) -> bool:
    """
    Whether the operator stores 'key' as passed when it is given as a keyword argument, e.g. a parameter read
    from **kwargs. Checked by creating the operator again with the argument, outside of any Dag
    """
    arguments = {k: v for k, v in user_kwargs.items() if k not in STRUCTURAL_TASK_PARAMETERS}
    arguments[key] = value
    arguments["task_id"] = f"{task.task_id}__dag_converter_check"
    try:
        check = type(task)(**arguments)
    except Exception:
        return False
    return not values_differ(getattr(check, key, NOTSET), value)


def warn_if_unverified_operator(task_type: str, full_operator_name: str, validator: ArgumentValidator):
    """
    Warn about operators missing from the verified operator list.

    The static check of the Dag file only sees classes whose names contain 'Operator' or 'Sensor', so the warning
    is repeated here for the remaining ones.
    """
    if "Operator" in task_type or "Sensor" in task_type:
        return
    if full_operator_name not in validator.get_allowed_operators():
        print_warning(f"Operator {task_type} has not been verified for conversion, converting anyway")


def get_operator_defaults(operator_class: type) -> dict[str, Any]:
    """Returns the constructor default of every parameter of 'operator_class', following the class MRO"""
    defaults = {}
    for cls in reversed(operator_class.__mro__):
        try:
            parameters = inspect.signature(cls.__init__).parameters
        except (TypeError, ValueError):
            continue
        for name, parameter in parameters.items():
            if parameter.default is not inspect.Parameter.empty:
                defaults[name] = parameter.default
    return defaults


def get_operator_parameters(operator_class_or_string: type[Operator] | str) -> list[str]:
    """
    Returns a list of all allowed parameters for a given operator.

    Args:
        operator_class_or_string: Either the operator class or a string like
                                 'airflow.providers.amazon.aws.operators.ec2.EC2TerminateInstanceOperator'

    Returns:
        list: List of parameter names that the operator accepts
    """
    # If it's a string, import the class
    if isinstance(operator_class_or_string, str):
        module_path, class_name = operator_class_or_string.rsplit(".", 1)
        try:
            module = importlib.import_module(module_path)
            operator_class = getattr(module, class_name)
        except (ImportError, AttributeError):
            return []
    else:
        operator_class = operator_class_or_string

    all_params = []

    for cls in operator_class.__mro__:
        if hasattr(cls, "__init__"):
            try:
                sig = inspect.signature(cls.__init__)
                params = [name for name in sig.parameters if name not in ["self", "args", "kwargs"]]
                all_params.extend(params)
            except Exception:
                continue

    # Remove duplicates while preserving order
    return list(dict.fromkeys(all_params))
