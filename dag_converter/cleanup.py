"""Module for cleaning up data structures before serialization."""

import dataclasses
import functools
import re
from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Any

import pendulum
from airflow.sdk.definitions._internal.types import ArgNotSet
from airflow.sdk.definitions.param import Param, ParamsDict
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from pendulum.tz.timezone import FixedTimezone

from dag_converter.conversion.exceptions import InvalidOperatorParameter

# DagBag imports Dag files under internal module names that start with this prefix
DAG_FILE_MODULE_PREFIX = "unusual_prefix_"

# Dict keys YAML and DagFactory can represent
KEY_TYPES = (str, int, float, bool, type(None))


class TypedValue(dict):
    """
    DagFactory '__type__' object produced by the cleanup. Its contents are already cleaned, and it is told apart
    from user dicts that contain a '__type__' key, which DagFactory would try to cast.
    """


def typed(type_path: str, args: list | None = None, **kwargs: Any) -> TypedValue:
    """DagFactory object recreated as type_path(*args, **kwargs). 'args' and 'kwargs' must be cleaned"""
    value = TypedValue(__type__=type_path)
    if args is not None:
        value["__args__"] = args
    value.update(kwargs)
    return value


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

    # Airflow's own string-valued enums (e.g. TriggerRule, OnFinishAction) are accepted as their plain value.
    # Other enums keep their type through '__type__' below
    if isinstance(obj, str) and isinstance(obj, Enum) and is_airflow_enum(obj):
        return obj.value

    if isinstance(obj, str) and not isinstance(obj, Enum):
        return str(obj)

    if isinstance(obj, TypedValue):
        return obj

    # Before the primitive check, IntEnum and IntFlag members are ints
    if isinstance(obj, Enum):
        return typed(get_type_path(obj), [get_cleanup_dag(obj.value)])

    if is_primitive(obj):
        return obj

    if isinstance(obj, PlainXComArg):
        pattern = r',\s*dag_id=[\'"][^\'"]*[\'"]'
        # Remove the dag_id parameter
        print("plain xcom arg:" + str(obj))
        return re.sub(pattern, "", str(obj))

    if isinstance(obj, ParamsDict):
        return {key: get_cleanup_dag(obj.get_param(key)) for key in obj}

    if isinstance(obj, Param):
        return get_param_value(obj)

    if isinstance(obj, dict):
        # Handle encoded values
        if "__var" in obj and "__type" in obj:
            # Special handling for None
            if obj["__var"] is None:
                return None
            return get_cleanup_dag(obj["__var"])

        if "__type__" in obj:
            raise InvalidOperatorParameter(
                "Dicts with a '__type__' key cannot be converted, DagFactory would create an object from them"
            )
        # Regular dictionary
        return {get_dict_key(k): get_cleanup_dag(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [get_cleanup_dag(item) for item in obj]

    # Other containers keep their type through DagFactory's '__type__', e.g. tuple([1, 2])
    if type(obj) in (tuple, set, frozenset):
        items = [get_cleanup_dag(item) for item in obj]
        if isinstance(obj, set | frozenset):
            items = sorted(items, key=repr)
        return typed(f"builtins.{type(obj).__name__}", [items])

    # Named tuples are created from their fields
    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return typed(get_type_path(obj), [get_cleanup_dag(item) for item in obj])

    if isinstance(obj, tuple | set | frozenset):
        raise_unsupported_value(obj)

    # Naive pendulum DateTimes default to fold=1, pendulum.naive restores them as they are
    if isinstance(obj, pendulum.DateTime) and obj.tzinfo is None:
        return typed(
            "pendulum.naive",
            [obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second, obj.microsecond],
            fold=obj.fold,
        )

    if isinstance(obj, datetime):
        custom_object = {
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
        # The second occurrence of a wall-clock time repeated when clocks go back
        if obj.fold:
            custom_object["fold"] = 1
        value = typed("datetime.datetime", **custom_object)
        # pendulum.instance turns the datetime back into a pendulum DateTime, keeping its timezone
        if isinstance(obj, pendulum.DateTime) and obj.tzinfo is not None:
            return typed("pendulum.instance", [value])
        return value

    if isinstance(obj, date):
        date_type = "pendulum.date" if isinstance(obj, pendulum.Date) else "datetime.date"
        return typed(date_type, [obj.year, obj.month, obj.day])

    if isinstance(obj, time):
        custom_object = {}
        if obj.tzinfo is not None:
            custom_object["tzinfo"] = get_timezone_value(obj.tzinfo, None)
        if obj.fold:
            custom_object["fold"] = 1
        return typed("datetime.time", [obj.hour, obj.minute, obj.second, obj.microsecond], **custom_object)

    if isinstance(obj, Decimal):
        return typed("decimal.Decimal", [str(obj)])

    # The concrete classes are in 'pathlib._local' on Python 3.13, which older versions do not have
    if isinstance(obj, PurePath):
        return typed(f"pathlib.{type(obj).__name__}", [str(obj)])

    if isinstance(obj, tzinfo):
        return get_timezone_value(obj, None)

    # DagFactory deep-copies the loaded configuration, which pendulum Intervals do not support
    if isinstance(obj, pendulum.Interval):
        raise InvalidOperatorParameter(
            "pendulum Interval values cannot be converted, DagFactory cannot copy them. Pass the start and end "
            "datetimes instead"
        )

    # Durations keep years and months as calendar units. In total seconds they would be fixed numbers of days
    if isinstance(obj, pendulum.Duration):
        calendar_seconds = pendulum.duration(years=obj.years, months=obj.months).total_seconds()
        return typed(
            "pendulum.duration",
            years=obj.years,
            months=obj.months,
            seconds=obj.total_seconds() - calendar_seconds,
        )

    if isinstance(obj, timedelta):
        return typed("datetime.timedelta", seconds=obj.total_seconds())

    if isinstance(obj, functools.partial):
        raise InvalidOperatorParameter(
            f"functools.partial of {getattr(obj.func, '__name__', obj.func)} cannot be converted. "
            "Use a module-level function instead"
        )

    if callable(obj) and hasattr(obj, "__name__"):
        function_name = obj.__name__
        # DagFactory imports callables as '<module>.<name>' with a single attribute lookup
        qualified_name = getattr(obj, "__qualname__", function_name)
        if function_name == "<lambda>" or "<locals>" in qualified_name or "." in qualified_name:
            raise InvalidOperatorParameter(
                f"Callable {qualified_name} cannot be converted. DagFactory imports callables by module and "
                "name, so use a module-level function instead of a lambda, nested function or method"
            )

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
        return typed(get_type_path(obj), **get_cleanup_dag(init_fields))

    # Other objects would have to be rebuilt from their attributes, which do not have to match their __init__
    # arguments, and writing them as str would change their type
    raise_unsupported_value(obj)


def raise_unsupported_value(obj: Any):
    raise InvalidOperatorParameter(
        f"Value of type {type(obj).__module__}.{type(obj).__qualname__} cannot be converted. Use primitive values, "
        "containers, enums, dataclasses, named tuples, datetime values, Decimal or paths"
    )


def is_airflow_enum(member: Enum) -> bool:
    """Enums defined by Airflow or its providers, whose operators accept their plain value"""
    return type(member).__module__.split(".")[0] == "airflow"


def get_dict_key(key: Any) -> Any:
    """Dict key DagFactory can load. Keys cannot carry a '__type__', so only Airflow's str enums are allowed"""
    if isinstance(key, str) and isinstance(key, Enum):
        if is_airflow_enum(key):
            return key.value
        raise InvalidOperatorParameter(
            f"Dict key {key!r} cannot be converted, YAML keys cannot keep the enum type. Use its value as the key"
        )
    if isinstance(key, KEY_TYPES) and not isinstance(key, Enum):
        return key
    raise InvalidOperatorParameter(f"Dict key {key!r} cannot be converted. Use str, int, float or bool keys")


def get_param_value(param: Param) -> Any:
    """Params with a schema or description are written as Param objects to keep their validation"""
    if not (param.schema or param.description):
        return get_cleanup_dag(param.value)
    args = None if isinstance(param.value, ArgNotSet) else [get_cleanup_dag(param.value)]
    kwargs = get_cleanup_dag(dict(param.schema))
    if param.description:
        kwargs["description"] = param.description
    return typed(get_type_path(param), args, **kwargs)


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


def get_timezone_value(tz: tzinfo, dt: datetime | None) -> TypedValue:
    """DagFactory '__type__' object that recreates the timezone"""
    return typed("pendulum.timezone", [get_timezone_name(tz, dt)])


def is_primitive(obj: Any) -> bool:
    """Check if an object is NOT a primitive type."""
    primitive_types = (int, float, bool, bytes, type(None))
    return isinstance(obj, primitive_types)
