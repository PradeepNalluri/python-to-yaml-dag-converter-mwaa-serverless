"""Run-time Connection and Variable lookups for converted Dags.

While a Dag file is parsed, ``patched`` makes ``Variable.get`` and the Connection lookups return marker strings.
``RuntimeLookups.resolve`` then turns markers in templated task fields into Jinja that reads the value on
MWAA Serverless at run time, and anything else into the local value. Imported only after the parse environment
is set up: importing Airflow's models loads the configured secrets backend (``parse_secrets``).
"""

import contextlib
import json
import keyword
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any
from unittest import mock

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.models.variable import Variable

from dag_converter.code_package import iter_strings
from dag_converter.console_config import print_warning

NO_DEFAULT = object()
MARKER_RE = re.compile(r"__dagconv_(?:var|conn)_\d+__", re.IGNORECASE)
CONN_FIELDS = ("host", "login", "password", "schema")
# Cosmos reads its dbt ls cache with default_var=None and treats a missing Variable as a cache miss
COSMOS_CACHE_PREFIX = "cosmos_cache__"
PLACEHOLDER = "placeholder"


def _reference(accessor: str, key: str) -> str:
    """``var.value``/``conn`` reference for Airflow's Jinja environment. Subscripts fall back to the accessor's
    attribute lookup, so like ``Variable.get``/``get_connection`` they raise for a missing key, unlike
    ``.get('key')``, which the sandbox still allows for ``__``-prefixed keys and ``get``."""
    if key.isidentifier() and not keyword.iskeyword(key) and not key.startswith("__") and key != "get":
        return f"{accessor}.{key}"
    if key.startswith("__") or key == "get":
        return f"{accessor}.get({key!r})"
    return f"{accessor}[{key!r}]"


@dataclass
class Lookup:
    kind: str  # "var" or "conn"
    key: str  # Variable key or connection id
    field: str | None = None  # connection field
    default: Any = NO_DEFAULT
    local: Any = NO_DEFAULT  # the locally defined value, if any

    @property
    def ident(self) -> tuple:
        return self.kind, self.key, self.field

    def concrete(self) -> Any:
        """The local value, else the Dag's default, else a placeholder."""
        if self.local is not NO_DEFAULT:
            return self.local
        return PLACEHOLDER if self.default is NO_DEFAULT else self.default

    def describe(self) -> str:
        if self.kind == "conn":
            return f"connection '{self.key}' ({self.field})"
        return f"Variable '{self.key}'"

    def shown(self) -> str:
        return "***" if self.field == "password" else repr(self.concrete())

    def jinja(self) -> str | None:
        """Jinja that reads the value at run time, None if the default cannot be written as a literal."""
        if self.kind == "conn":
            return f"{{{{ {_reference('conn', self.key)}.{self.field} }}}}"
        if self.default is NO_DEFAULT:
            ref = _reference("var.value", self.key)
        elif self.default is None or isinstance(self.default, str | int | bool):
            ref = f"var.value.get({self.key!r}, {self.default!r})"
        else:
            return None
        return f"{{{{ {ref} }}}}"


class _Marker(str):
    """A marker string. Comparing it or converting it to a number while parsing uses the concrete value
    and records the lookup as used in Dag logic."""

    __hash__ = str.__hash__

    def _concrete(self) -> Any:
        lookups = _active
        if lookups is None or not lookups.parsing or str(self) not in lookups.markers:
            return str(self)
        lookups.used_in_logic.add(str(self))
        return lookups.markers[str(self)].concrete()

    def __eq__(self, other):
        return self._concrete() == other

    def __ne__(self, other):
        return self._concrete() != other

    def __int__(self):
        return int(self._concrete())

    def __float__(self):
        return float(self._concrete())


_active: "RuntimeLookups | None" = None


class RuntimeLookups:
    """Lookups made while parsing one Dag file. ``frozen`` lookups (``Lookup.ident``) return their concrete
    value instead of a marker."""

    def __init__(self, frozen: Iterable[tuple] = ()):
        self.frozen = set(frozen)
        self.markers: dict[str, Lookup] = {}
        self.used_in_logic: set[str] = set()  # markers compared or converted while parsing
        self.concrete_used: dict[tuple, tuple[Lookup, str]] = {}  # ident -> (lookup, reason)
        self.conn_ids: set[str] = set()
        self.var_keys: set[str] = set()
        self.parsing = True

    def _marker(self, lookup: Lookup) -> Any:
        if not self.parsing:  # e.g. a hook property read while converting: not part of the Dag definition
            return lookup.concrete()
        if lookup.ident in self.frozen:
            self.concrete_used[lookup.ident] = (lookup, "used in Dag logic at conversion time")
            return lookup.concrete()
        marker = f"__dagconv_{lookup.kind}_{len(self.markers)}__"
        self.markers[marker] = lookup
        return _Marker(marker)

    def variable(self, key: str, default: Any, deserialize_json: bool, original) -> Any:
        if key.startswith(COSMOS_CACHE_PREFIX):
            args = () if default is NO_DEFAULT else (default,)
            return original(key, *args, deserialize_json=deserialize_json)
        self.var_keys.add(key)
        value = Variable.get_variable_from_secrets(key)
        lookup = Lookup("var", key, default=default, local=NO_DEFAULT if value is None else value)
        if deserialize_json:
            if self.parsing:
                self.concrete_used[lookup.ident] = (lookup, "read with deserialize_json=True")
            return json.loads(value) if value is not None else lookup.concrete()
        return self._marker(lookup)

    def connection(self, conn_id: str, conn_class: type, original) -> Any:
        self.conn_ids.add(conn_id)
        try:
            local = original(conn_id)
        except AirflowNotFoundException:
            local = None
        fields = {
            field: self._marker(Lookup("conn", conn_id, field, local=getattr(local, field) if local else NO_DEFAULT))
            for field in CONN_FIELDS
        }
        return conn_class(
            conn_id=conn_id,
            conn_type=local.conn_type if local else "generic",
            port=local.port if local else None,
            extra=local.extra if local else None,
            **fields,
        )

    def consumed(self, dag_dicts: Iterable[dict], ids: Iterable[str]) -> set[tuple]:
        """Lookups the Dag consumed while parsing, so that a marker cannot stand in for them: Variables whose
        marker is not in the output, and markers that were transformed (e.g. upper-cased) or used in an id.
        Markers compared or converted while parsing already used the concrete value; they are included only
        so that a second parse uses it too."""
        text = "\n".join(value for dag_dict in dag_dicts for _, _, value in iter_strings(dag_dict))
        found = set(MARKER_RE.findall(text))
        kept = found & self.markers.keys()
        changed = {marker.lower() for marker in found - kept} | {m.lower() for m in MARKER_RE.findall(" ".join(ids))}
        missing = {marker for marker, lookup in self.markers.items() if lookup.kind == "var" and marker not in kept}
        if not (changed | missing) & self.markers.keys():
            return set()
        return {self.markers[m].ident for m in changed | missing | self.used_in_logic if m in self.markers}

    def warn_logic(self) -> None:
        used = dict(self.concrete_used)
        for marker in self.used_in_logic:
            lookup = self.markers[marker]
            used.setdefault(lookup.ident, (lookup, "used in Dag logic at conversion time"))
        if used:
            lines = [
                f"  {lookup.describe()}: {reason}; the YAML reflects {lookup.shown()}"
                for lookup, reason in sorted(used.values(), key=lambda item: item[0].ident)
            ]
            print_warning(
                "These values were fixed at conversion time:\n" + "\n".join(lines) + "\nRestructure the Dag to "
                "read them inside a templated field (e.g. a Jinja template in bash_command) to make them dynamic.",
                markup=False,
            )

    def resolve(self, dag_dict: dict, dag_object: Any) -> dict:
        """Replace markers: in templated task fields with Jinja, elsewhere with the concrete value (warned)."""
        template_fields = {task.task_id: set(task.template_fields or ()) for task in getattr(dag_object, "tasks", [])}
        fixed: list[str] = []

        def replace(value: str, templated: bool, where: str) -> Any:
            def sub(match: re.Match) -> str:
                lookup = self.markers.get(match[0])
                jinja = lookup.jinja() if lookup and templated else None
                if lookup and not jinja:
                    fixed.append(f"  {where}: {lookup.describe()} = {lookup.shown()}")
                    return str(lookup.concrete())
                return jinja or match[0]

            if not templated and value in self.markers:
                sub(MARKER_RE.match(value))
                return self.markers[value].concrete()  # keeps the type of a default
            return MARKER_RE.sub(sub, value)

        def walk(obj: Any, templated: bool, where: str) -> Any:
            if isinstance(obj, dict):
                templated = templated and "__type__" not in obj  # nested objects are not rendered
                return {key: walk(value, templated, f"{where}.{key}") for key, value in obj.items()}
            if isinstance(obj, list):
                return [walk(value, templated, where) for value in obj]
            return replace(obj, templated, where) if isinstance(obj, str) and MARKER_RE.search(obj) else obj

        def walk_task(task_id: str, task: dict) -> dict:
            fields = template_fields.get(task_id, ())
            return {arg: walk(value, arg in fields, f"{task_id}.{arg}") for arg, value in task.items()}

        resolved = {
            dag_id: {
                key: {task_id: walk_task(task_id, task) for task_id, task in value.items()}
                if key == "tasks"
                else walk(value, False, key)
                for key, value in dag.items()
            }
            if isinstance(dag, dict)
            else dag
            for dag_id, dag in dag_dict.items()
        }
        if fixed:
            print_warning(
                "These values cannot be read at run time (the field is not templated, or the default is not a "
                "literal); the YAML has the conversion-time value:\n" + "\n".join(fixed),
                markup=False,
            )
        return resolved


def assert_no_markers(text: str) -> None:
    match = MARKER_RE.search(text)
    if match:
        raise RuntimeError(f"Internal error: unresolved parse-time marker {match[0]} in the converted YAML")


@contextlib.contextmanager
def patched(frozen: Iterable[tuple] = ()) -> Iterator[RuntimeLookups]:
    """Patch Variable and Connection lookups (``airflow.models`` and ``airflow.sdk``) to return markers."""
    from airflow.sdk.definitions._internal.types import NOTSET
    from airflow.sdk.definitions.connection import Connection as SdkConnection
    from airflow.sdk.definitions.variable import Variable as SdkVariable

    global _active
    lookups = RuntimeLookups(frozen)
    get_variable, get_connection = Variable.get, Connection.get_connection_from_secrets

    def models_variable(cls, key, default_var=NO_DEFAULT, deserialize_json=False):
        return lookups.variable(key, default_var, deserialize_json, get_variable)

    def sdk_variable(cls, key, default=NOTSET, deserialize_json=False):
        return lookups.variable(key, NO_DEFAULT if default is NOTSET else default, deserialize_json, get_variable)

    with (
        mock.patch.object(Variable, "get", classmethod(models_variable)),
        mock.patch.object(SdkVariable, "get", classmethod(sdk_variable)),
        mock.patch.object(
            Connection,
            "get_connection_from_secrets",
            classmethod(lambda cls, conn_id: lookups.connection(conn_id, Connection, get_connection)),
        ),
        mock.patch.object(
            SdkConnection,
            "get",
            classmethod(lambda cls, conn_id: lookups.connection(conn_id, SdkConnection, get_connection)),
        ),
    ):
        _active = lookups
        try:
            yield lookups
        finally:
            _active = None
