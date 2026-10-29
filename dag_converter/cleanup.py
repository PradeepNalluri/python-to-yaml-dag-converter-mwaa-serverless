"""Module for cleaning up data structures before serialization."""

import dataclasses
import functools
import re
from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any

from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from pendulum.tz.timezone import FixedTimezone

from dag_converter.conversion.exceptions import InvalidOperatorParameter

# DagBag imports Dag files under internal module names that start with this prefix
DAG_FILE_MODULE_PREFIX = "unusual_prefix_"


def is_dag_file_module(module_name: str) -> bool:
    """Check if 'module_name' is DagBag's internal name for the Dag file, which cannot be imported elsewhere"""
    return module_name.startswith(DAG_FILE_MODULE_PREFIX)


def get_type_path(obj: Any) -> str:
    """Import path of the class of 'obj' for DagFactory's '__type__'"""
    cls = obj.__class__
    if is_dag_file_module(cls.__module__):
        raise InvalidOperatorParameter(
            f"{cls.__name__} is defined in the Dag file. "
            "Move it to a module that can be imported where the YAML Dag runs"
        )
    return f"{cls.__module__}.{cls.__qualname__}"


def get_cleanup_dag(obj: Any) -> Any:
    """Recursively clean data structure of non-serializable objects.

    Args:
        obj: Any Python object to clean up

    Returns:
        Cleaned up version of the object suitable for YAML serialization

    The function handles:
    - Encoded values with __var and __type
    - Basic Python containers (dict, list, tuple, set)
    - Custom objects (converted to Dag factory YAML object format)
    - Enums (converted to Dag factory YAML object format)
    - Date/time objects (converted to custom Python object)
    - None values
    - Nested structures
    """
    if obj is None:
        return None

    if isinstance(obj, str) and "xcom_pull" in obj:
        pattern = r',\s*dag_id=[\'"][^\'"]*[\'"]'
        print("xcom arg:" + str(obj))
        return re.sub(pattern, "", obj)

    # String-valued enums (e.g. OnFinishAction) are accepted by operators as their plain value.
    # str() would render them as "ClassName.MEMBER"
    if isinstance(obj, str) and isinstance(obj, Enum):
        return obj.value

    if isinstance(obj, str):
        return str(obj)

    # Before the primitive check, IntEnum and IntFlag members are ints
    if isinstance(obj, Enum):
        return {"__type__": get_type_path(obj), "__args__": [obj.value]}

    if is_primitive(obj):
        return obj

    if isinstance(obj, PlainXComArg):
        pattern = r',\s*dag_id=[\'"][^\'"]*[\'"]'
        # Remove the dag_id parameter
        print("plain xcom arg:" + str(obj))
        return re.sub(pattern, "", str(obj))

    if isinstance(obj, ParamsDict):
        return obj.dump()

    if isinstance(obj, dict):
        # Handle encoded values
        if "__var" in obj and "__type" in obj:
            # Special handling for None
            if obj["__var"] is None:
                return None
            return get_cleanup_dag(obj["__var"])

        # Regular dictionary
        return {k: get_cleanup_dag(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [get_cleanup_dag(item) for item in obj]

    # Other containers keep their type through DagFactory's '__type__', e.g. tuple([1, 2])
    if isinstance(obj, tuple | set | frozenset):
        items = [get_cleanup_dag(item) for item in obj]
        if isinstance(obj, set | frozenset):
            items = sorted(items, key=repr)
        return {"__type__": f"builtins.{type(obj).__name__}", "__args__": [items]}

    if isinstance(obj, datetime):
        custom_object = {
            "__type__": "datetime.datetime",
            "year": obj.year,
            "month": obj.month,
            "day": obj.day,
            "hour": obj.hour,
            "minute": obj.minute,
            "second": obj.second,
            "microsecond": obj.microsecond,
        }
        custom_object = {key: value for key, value in custom_object.items() if value}
        if obj.tzinfo is not None:
            custom_object["tzinfo"] = get_timezone_value(obj.tzinfo, obj)
        return custom_object

    if isinstance(obj, date):
        return {"__type__": "datetime.date", "__args__": [obj.year, obj.month, obj.day]}

    if isinstance(obj, time):
        custom_object = {"__type__": "datetime.time", "__args__": [obj.hour, obj.minute, obj.second, obj.microsecond]}
        if obj.tzinfo is not None:
            custom_object["tzinfo"] = get_timezone_value(obj.tzinfo, None)
        return custom_object

    if isinstance(obj, Decimal):
        return {"__type__": "decimal.Decimal", "__args__": [str(obj)]}

    if isinstance(obj, PurePath):
        return {"__type__": get_type_path(obj), "__args__": [str(obj)]}

    if isinstance(obj, timedelta):
        return {"__type__": "datetime.timedelta", "seconds": obj.total_seconds()}

    if isinstance(obj, functools.partial):
        raise InvalidOperatorParameter(
            f"functools.partial of {getattr(obj.func, '__name__', obj.func)} cannot be converted. "
            "Use a module-level function instead"
        )

    if callable(obj) and hasattr(obj, "__name__"):
        function_name = obj.__name__

        if hasattr(obj, "__module__"):
            module_name = obj.__module__
            # Clean up unusual prefixes and extract actual module name
            if "unusual_prefix_" in module_name:
                parts = module_name.split("_")
                input_file_name = parts[3]
                for i in range(4, len(parts)):
                    input_file_name += f"_{parts[i]}"
            else:
                input_file_name = module_name
        else:
            input_file_name = "unknown"

        return f"{input_file_name}.{function_name}"

    # Dataclasses are rebuilt from their __init__ fields
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        init_fields = {field.name: getattr(obj, field.name) for field in dataclasses.fields(obj) if field.init}
        return {"__type__": get_type_path(obj), **get_cleanup_dag(init_fields)}

    # Other objects would be rebuilt from __dict__, which does not have to match their __init__ arguments
    if hasattr(obj, "__dict__"):
        raise InvalidOperatorParameter(
            f"Value of type {obj.__module__}.{obj.__class__.__name__} cannot be converted. "
            "Use primitive values, enums, dataclasses or datetime values"
        )

    return str(obj)


def get_timezone_name(tz: tzinfo, dt: datetime | None) -> str | int:
    """IANA name of the timezone, or its UTC offset in seconds for fixed offsets. pendulum.timezone accepts both"""
    name = getattr(tz, "key", None)  # zoneinfo
    if name is None and not isinstance(tz, FixedTimezone):
        name = getattr(tz, "name", None)  # pendulum
    if name:
        return name
    offset = tz.utcoffset(dt)
    if offset is None:
        raise InvalidOperatorParameter(f"Timezone {tz!r} cannot be converted")
    return "UTC" if not offset else int(offset.total_seconds())


def get_timezone_value(tz: tzinfo, dt: datetime | None) -> dict[str, Any]:
    """DagFactory '__type__' object that recreates the timezone"""
    return {"__type__": "pendulum.timezone", "__args__": [get_timezone_name(tz, dt)]}


def is_primitive(obj: Any) -> bool:
    """Check if an object is NOT a primitive type."""
    primitive_types = (int, float, bool, bytes, type(None))
    return isinstance(obj, primitive_types)
