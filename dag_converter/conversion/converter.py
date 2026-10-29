import inspect
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.models.dag import DAG
from airflow.sdk.definitions._internal.types import ArgNotSet
from airflow.sdk.definitions.param import Param

from dag_converter.cleanup import get_timezone_name, get_type_path
from dag_converter.conversion.arguments import get_dag_argument_value
from dag_converter.conversion.default_args import convert_default_args
from dag_converter.conversion.schedule import convert_schedule
from dag_converter.conversion.tasks import convert_tasks
from dag_converter.schema_parser import ArgumentValidator
from dag_converter.taskflow_parser import TaskFlowAnalyzer


def get_dag_default_values() -> dict:
    """
    Returns a dictionary of default values for DAG class.
    
    Returns:
        dict: Dictionary of parameter names and their default values
    """
    defaults = {}
    
    try:
        sig = inspect.signature(DAG.__init__)
        for name, param in sig.parameters.items():
            if name not in ["self", "args", "kwargs"] and param.default is not inspect.Parameter.empty:
                defaults[name] = param.default
    except Exception:
        pass
    
    return defaults


def is_dag_default_value(key: str, value, dag_defaults: dict) -> bool:
    """
    Check if a DAG-level value is a default that should be excluded from output.
    """
    if key not in dag_defaults:
        return False
    
    default_value = dag_defaults[key]
    
    # Only filter out values that are still NOTHING (truly not set)
    if hasattr(value, 'name') and value.name == 'NOTHING':
        return True
    
    # For regular defaults, filter if values match
    return value == default_value


def convert_params(dag_object: DAG) -> dict[str, Any]:
    """Dag params. Params with a schema or description are written as Param objects to keep their validation"""
    params_obj = getattr(dag_object, "params", None)
    params_dict = {}
    if not params_obj or not hasattr(params_obj, "items"):
        return params_dict
    get_param = getattr(params_obj, "get_param", None)
    for key, value in params_obj.items():
        param = get_param(key) if get_param else value
        if not isinstance(param, Param):
            params_dict[key] = value
        elif param.schema or param.description:
            converted = {"__type__": get_type_path(param)}
            if not isinstance(param.value, ArgNotSet):
                converted["__args__"] = [param.value]
            converted.update(param.schema)
            if param.description:
                converted["description"] = param.description
            params_dict[key] = converted
        else:
            params_dict[key] = param.value
    return params_dict


# Dag fields DagFactory reads in the Dag 'timezone' (and default_args 'timezone'), replacing their tzinfo
DAG_TIMEZONE_FIELDS = ("start_date", "end_date")


def convert_dag_timezone(dag_object: DAG, converted_dag: dict[str, Any]) -> None:
    """
    Write start_date and end_date as wall-clock times in the Dag timezone.

    DagFactory replaces the tzinfo of these fields with the 'timezone' setting (UTC by default) instead of
    converting them, and the Dag timezone also decides how cron schedules are interpreted.
    """
    dag_timezone = dag_object.timezone
    timezone_name = get_timezone_name(dag_timezone, None)

    sections = [converted_dag]
    if isinstance(converted_dag.get("default_args"), dict):
        sections.append(converted_dag["default_args"])
    for section in sections:
        local_dates = False
        for field in DAG_TIMEZONE_FIELDS:
            value = section.get(field)
            if isinstance(value, datetime) and value.tzinfo is not None:
                section[field] = value.astimezone(dag_timezone).replace(tzinfo=None)
                local_dates = True
        if local_dates and timezone_name != "UTC":
            section["timezone"] = timezone_name


def get_converted_format(
    taskflow_parser: TaskFlowAnalyzer, dag_object: DAG, dag_file_path: Path, validator: ArgumentValidator
):
    """Return the DagFactory-compatible format of the Dag in dictionary format"""
    dag_id = getattr(dag_object, "_dag_id", None)
    # Taskflow may store dag_id as internal parameter with leading '_'
    if not dag_id:
        dag_id = getattr(dag_object, "dag_id", None)

    params_dict = convert_params(dag_object)

    # Convert Dag information
    converted_dag = {
        dag_id: {
            "dag_id": dag_id,
            "params": params_dict,
            "default_args": convert_default_args(getattr(dag_object, "default_args", {}), validator),
            "schedule": convert_schedule(dag_object),
            "tasks": {},
        }
    }

    # Get DAG default values to filter them out
    dag_defaults = get_dag_default_values()

    # Filter out extraneous Dag fields
    for key in dir(dag_object):
        value = getattr(dag_object, key, None)
        # Only keep non-internal fields and valid fields
        if key not in converted_dag[dag_id] and not key.startswith("_") and validator.validate_field("dag", key, value):
            # Skip keywords that were not set by user (None values or default values)
            if value is None or is_dag_default_value(key, value, dag_defaults):
                pass
            # Airflow stores tags as a set, DagFactory appends to a list
            elif key == "tags":
                converted_dag[dag_id][key] = sorted(value)
            else:
                value = get_dag_argument_value(key, value)
                if value is not None:
                    converted_dag[dag_id][key] = value

    convert_dag_timezone(dag_object, converted_dag[dag_id])

    # Convert tasks
    converted_dag[dag_id]["tasks"] = convert_tasks(taskflow_parser, dag_object, dag_file_path, validator)

    return converted_dag
