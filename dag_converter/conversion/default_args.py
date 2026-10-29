from typing import Any

from airflow.sdk import BaseOperator

from dag_converter.conversion.arguments import get_dag_argument_value
from dag_converter.conversion.exceptions import InvalidDefaultArg
from dag_converter.dagbag import get_constructor_parameter_names
from dag_converter.schema_parser import ArgumentValidator


def convert_default_args(args: dict[str, Any], validator: ArgumentValidator) -> dict[str, Any]:
    converted = {}

    operator_parameters = get_constructor_parameter_names(BaseOperator)
    for key, value in args.items():
        # Airflow passes default_args to operator constructors, so any BaseOperator parameter is valid
        if key in operator_parameters or validator.validate_field("dag", "default_args", key):
            converted[key] = get_dag_argument_value(key, value)
        else:
            raise InvalidDefaultArg(f"Default Argument '{key}' is not supported")

    return converted
