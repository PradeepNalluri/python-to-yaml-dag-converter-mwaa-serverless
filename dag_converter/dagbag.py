import copy
import functools
import inspect
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.sdk import BaseOperator


class NoDagFoundError(Exception):
    """The file imports without errors but does not define a Dag"""


# Attribute holding the keyword arguments the Dag file passed to the operator class
USER_KWARGS_ATTRIBUTE = "_dag_converter_user_kwargs"
# Attribute holding the constructor parameter attributes of the operator right after __init__
INIT_ATTRIBUTES_ATTRIBUTE = "_dag_converter_init_attributes"


@functools.cache
def get_constructor_parameter_names(operator_class: type) -> frozenset[str]:
    """Names of the named parameters of every __init__ in the class MRO"""
    names = set()
    for cls in operator_class.__mro__:
        try:
            parameters = inspect.signature(cls.__init__).parameters.values()
        except (TypeError, ValueError):
            continue
        names.update(p.name for p in parameters if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD))
    names.discard("self")
    return frozenset(names)


def get_assigned_attributes(operator: Any) -> dict[str, Any]:
    """
    Constructor parameter attributes the Dag file changed after creating the operator,
    e.g. 'delete_bucket.trigger_rule = TriggerRule.ALL_DONE'
    """
    init_attributes = vars(operator).get(INIT_ATTRIBUTES_ATTRIBUTE)
    if init_attributes is None:
        return {}
    assigned = {}
    for name, init_value in init_attributes.items():
        value = getattr(operator, name, None)
        try:
            changed = bool(value != init_value)
        except Exception:
            changed = value is not init_value
        if changed:
            assigned[name] = value
    return assigned


def _snapshot(value: Any) -> Any:
    """Shallow copy of containers, so in-place changes after __init__ are detected"""
    if isinstance(value, list | dict | set):
        return copy.copy(value)
    return value


@contextmanager
def record_operator_arguments() -> Iterator[None]:
    """
    Record the keyword arguments of every operator instantiation on the operator.

    BaseOperator._BaseOperator__init_kwargs also contains the arguments subclasses pass to their base classes
    (e.g. a BashOperator subclass that builds bash_command), which must not be written to the YAML: DagFactory
    calls the subclass again, and it would get them twice.
    """
    metaclass = type(BaseOperator)
    original_call = metaclass.__dict__.get("__call__")
    parent_call = metaclass.__call__

    def recording_call(cls, *args, **kwargs):
        operator = parent_call(cls, *args, **kwargs)
        object.__setattr__(operator, USER_KWARGS_ATTRIBUTE, dict(kwargs))
        init_attributes = {
            name: _snapshot(getattr(operator, name))
            for name in get_constructor_parameter_names(type(operator))
            if name in vars(operator)
        }
        object.__setattr__(operator, INIT_ATTRIBUTES_ATTRIBUTE, init_attributes)
        return operator

    metaclass.__call__ = recording_call
    try:
        yield
    finally:
        if original_call is None:
            del metaclass.__call__
        else:
            metaclass.__call__ = original_call


def get_dag_object(dag_file_path: Path) -> list[DAG]:
    """Load the Dags using DagBag and retrieve the Dag objects"""
    dag_file_path = dag_file_path.resolve()

    # Add the DAG file's directory to sys.path so sibling modules can be imported since
    # for python there can be more than one module
    dag_dir = str(dag_file_path.parent)
    if dag_dir not in sys.path:
        sys.path.insert(0, dag_dir)

    with record_operator_arguments():
        dagbag = DagBag(dag_folder=dag_file_path, include_examples=False)
    found_dags = list(dagbag.dags.values())

    if found_dags:
        return found_dags

    if dagbag.import_errors:
        details = "\n".join(f"{path}:\n{err}" for path, err in dagbag.import_errors.items())
        raise Exception(
            f"Failed to generate dag object for file {dag_file_path}. DagBag import errors:\n{details}"
        )

    raise NoDagFoundError(f"Failed to generate dag object for file {dag_file_path}. It does not define a Dag")


def get_dag_factory_object(dag_file_path: Path) -> DAG:
    """Retrieve the Dag Object created by DagFactory during the validation step"""
    dagbag = DagBag(dag_folder=dag_file_path, include_examples=False)
    for dag in dagbag.dags.values():
        # DagBag may contain the original Dag generated from the Python file,
        # only retrieve the DagFactory-generated Dag
        if getattr(dag, "is_dagfactory_auto_generated", None):
            return dag

    if dagbag.import_errors:
        details = "\n".join(f"{path}:\n{err}" for path, err in dagbag.import_errors.items())
        raise Exception(
            f"Failed to generate dag object for file {dag_file_path}. DagBag import errors:\n{details}"
        )

    raise Exception(
        f"Failed to generate dag object for file {dag_file_path}. "
        f"DagBag loaded {len(dagbag.dags)} DAG(s) but none were marked is_dagfactory_auto_generated."
    )
