"""Custom operator conversions, checked by loading the converted YAML with DagFactory and comparing operators."""

import shutil
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml
from airflow.models.dagbag import DagBag

from dag_converter.conversion.exceptions import InvalidOperatorError, InvalidOperatorParameter
from dag_converter.conversion_manager import ConversionManager
from dag_converter.dagbag import get_dag_object

OPS_MODULE = '''
import collections
import dataclasses
import enum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator


class KwargsOperator(BaseOperator):
    """Params consumed only through **kwargs"""

    def __init__(self, **kwargs):
        self.foo = kwargs.pop("foo")
        self.extra = kwargs.pop("extra", 0)
        super().__init__(**kwargs)

    def execute(self, context):
        return f"{self.foo}|{self.extra}"


class PrefixOperator(BaseOperator):
    """__init__ derives the stored values from the arguments"""

    def __init__(self, prefix="data", timeout=60, **kwargs):
        super().__init__(**kwargs)
        self.prefix = f"s3://bucket/{prefix}/"
        self.timeout = timeout * 2

    def execute(self, context):
        return self.prefix


class EchoBashOperator(BashOperator):
    """Builds the base operator argument itself"""

    def __init__(self, word, repeat=1, **kwargs):
        super().__init__(bash_command=" && ".join([f"echo {word}"] * repeat), **kwargs)
        self.word = word


def multiply(x, multiplier):
    return x * multiplier


class MultiplyOperator(PythonOperator):
    def __init__(self, x, multiplier=2, **kwargs):
        super().__init__(python_callable=multiply, op_kwargs={"x": x, "multiplier": multiplier}, **kwargs)


class AddOperator(BaseOperator):
    def __init__(self, x, y=0, tag="", **kwargs):
        super().__init__(**kwargs)
        self.x, self.y, self.tag = x, y, tag

    def execute(self, context):
        return f"{self.tag}:{self.x + self.y}"


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


@dataclasses.dataclass
class Point:
    x: int
    y: int = 0


class Plain:
    def __init__(self, a, b=0):
        self.a = a
        self._b = b


class PayloadOperator(BaseOperator):
    def __init__(self, payload, **kwargs):
        super().__init__(**kwargs)
        self.payload = payload

    def execute(self, context):
        return str(self.payload)


class Greeter(BaseOperator):
    """Class name without 'Operator' or 'Sensor'"""

    def execute(self, context):
        return "hi"


class DerivedOperator(BaseOperator):
    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.derived = value * 2

    def execute(self, context):
        return self.derived


class SelfUpdatingOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.state = "initial"

    def refresh(self):
        self.state = "refreshed"
        self.state = "initial"

    def execute(self, context):
        return self.state


class NormalizingOperator(BaseOperator):
    def __init__(self, options=None, **kwargs):
        super().__init__(**kwargs)
        self.options = options or {}

    def execute(self, context):
        return self.options


class Shape(enum.Enum):
    SQUARE = (1, 1)
    LINE = (1, 0)


class Key(str, enum.Enum):
    A = "a"


Pair = collections.namedtuple("Pair", ["left", "right"])


class Slotted:
    __slots__ = ("a",)

    def __init__(self, a):
        self.a = a


class Helpers:
    @staticmethod
    def triple(x):
        return x * 3


def record(context):
    pass


def record2(context):
    pass
'''

DAG_HEADER = """
from datetime import datetime
import functools

from airflow import DAG

import {ops} as m
from {ops} import *

with DAG(dag_id="{dag_id}", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
"""


class CustomOperatorTestCase(unittest.TestCase):
    _counter = 0

    def setUp(self):
        self.work_dir = Path(tempfile.mkdtemp())
        self.output_dir = self.work_dir / "out"
        # Unique module and Dag names so cached imports do not leak between tests
        CustomOperatorTestCase._counter += 1
        self.ops = f"custom_ops_{CustomOperatorTestCase._counter}"
        self.dag_id = f"custom_dag_{CustomOperatorTestCase._counter}"
        (self.work_dir / f"{self.ops}.py").write_text(OPS_MODULE)

    def tearDown(self):
        shutil.rmtree(self.work_dir)
        sys.modules.pop(self.ops, None)
        if str(self.work_dir) in sys.path:
            sys.path.remove(str(self.work_dir))

    def _write_dag(self, body: str) -> Path:
        header = DAG_HEADER.format(ops=self.ops, dag_id=self.dag_id)
        return self._write_source(header + textwrap.indent(textwrap.dedent(body), "    "))

    def _write_source(self, source: str) -> Path:
        dag_file = self.work_dir / f"{self.dag_id}_file.py"
        dag_file.write_text(source)
        return dag_file

    def convert(self, body: str):
        """Convert a Dag with the given task definitions, see convert_source"""
        return self._convert_file(self._write_dag(body))

    def convert_source(self, source: str):
        """
        Convert a Dag file. '{ops}' and '{dag_id}' in 'source' are replaced with the test's module and Dag id.

        Returns (original Dag, Dag loaded from the YAML by DagFactory, YAML of the Dag)
        """
        source = textwrap.dedent(source).replace("{ops}", self.ops).replace("{dag_id}", self.dag_id)
        dag_file = self._write_source(source)
        return self._convert_file(dag_file, whole_dag=True)

    def _convert_file(self, dag_file: Path, whole_dag: bool = False):
        ConversionManager(s3_bucket=None).start_conversion_process(
            dag_file_path=dag_file, output_dir=self.output_dir, user_validate=True
        )
        original = get_dag_object(dag_file)[0]

        yaml_path = self.output_dir / f"{self.dag_id}.yaml"
        loader_dir = self.work_dir / "loader"
        loader_dir.mkdir()
        (loader_dir / "load_converted.py").write_text(
            "from airflow import DAG\nfrom dagfactory import load_yaml_dags\n"
            f"load_yaml_dags(globals_dict=globals(), config_filepath=r'{yaml_path}')\n"
        )
        dagbag = DagBag(dag_folder=loader_dir, include_examples=False)
        self.assertEqual(dagbag.import_errors, {})
        converted = dagbag.dags[self.dag_id]
        converted_yaml = yaml.safe_load(yaml_path.read_text())[self.dag_id]
        return original, converted, converted_yaml if whole_dag else converted_yaml["tasks"]


class TestTaskArguments(CustomOperatorTestCase):
    def test_kwargs_only_parameters_are_kept(self):
        """R2: parameters popped from **kwargs are not in any signature"""
        original, converted, _ = self.convert('KwargsOperator(task_id="mixed", foo="F2", extra=7)\n')

        self.assertEqual(converted.get_task("mixed").foo, "F2")
        self.assertEqual(converted.get_task("mixed").extra, original.get_task("mixed").extra)

    def test_values_derived_in_init_are_not_written_back(self):
        """R3: the converted operator runs __init__ again, so only the original arguments may be written"""
        original, converted, tasks = self.convert(
            'PrefixOperator(task_id="p_default")\nPrefixOperator(task_id="p_explicit", prefix="raw", timeout=5)\n'
        )

        for task_id in ("p_default", "p_explicit"):
            self.assertEqual(converted.get_task(task_id).prefix, original.get_task(task_id).prefix)
            self.assertEqual(converted.get_task(task_id).timeout, original.get_task(task_id).timeout)
        self.assertNotIn("prefix", tasks["p_default"])

    def test_subclass_building_base_arguments(self):
        """R4: subclasses that fill in base operator arguments must not get them twice"""
        original, converted, tasks = self.convert('EchoBashOperator(task_id="echo", word="hi", repeat=2)\n')

        self.assertEqual(converted.get_task("echo").bash_command, original.get_task("echo").bash_command)
        self.assertNotIn("bash_command", tasks["echo"])
        self.assertEqual(tasks["echo"]["word"], "hi")

    def test_pass_through_subclass_keeps_base_arguments(self):
        original, converted, tasks = self.convert('EchoBashOperator.__base__(task_id="plain", bash_command="echo x")\n')

        self.assertEqual(tasks["plain"]["bash_command"], "echo x")

    def test_python_operator_subclass_setting_python_callable(self):
        """DagFactory requires python_callable in the YAML for PythonOperator subclasses"""
        with self.assertRaisesRegex(InvalidOperatorError, "python_callable"):
            self.convert('MultiplyOperator(task_id="mul", x=3, multiplier=5)\n')

    def test_airflow_3_trigger_rules(self):
        """R5: trigger rules missing from the schema enum were reset to all_success"""
        rules = ["always", "one_done", "none_failed_min_one_success", "all_skipped", "all_done_setup_success"]
        body = "".join(f'AddOperator(task_id="{rule}", x=1, trigger_rule="{rule}")\n' for rule in rules)

        _, converted, _ = self.convert(body)

        for rule in rules:
            self.assertEqual(converted.get_task(rule).trigger_rule, rule)

    def test_attributes_assigned_after_creation(self):
        """AWS example Dags set e.g. 'delete_bucket.trigger_rule = TriggerRule.ALL_DONE' after creating the task"""
        original, converted, tasks = self.convert(
            'cleanup = AddOperator(task_id="cleanup", x=1)\ncleanup.trigger_rule = "all_done"\ncleanup.y = 5\n'
            'derived = PrefixOperator(task_id="derived")\n'
        )

        self.assertEqual(tasks["cleanup"]["trigger_rule"], "all_done")
        self.assertEqual(converted.get_task("cleanup").trigger_rule, "all_done")
        self.assertEqual(converted.get_task("cleanup").y, 5)
        # Values __init__ derived are not assignments
        self.assertNotIn("prefix", tasks["derived"])
        self.assertEqual(converted.get_task("derived").prefix, original.get_task("derived").prefix)

    def test_inherited_default_args_are_not_repeated(self):
        self._write_dag("")
        dag_file = self.work_dir / f"{self.dag_id}_file.py"
        dag_file.write_text(
            dag_file.read_text().replace("catchup=False)", 'catchup=False, default_args={"retries": 3})')
            + '    AddOperator(task_id="inherits", x=1)\n    AddOperator(task_id="overrides", x=1, retries=5)\n'
        )
        ConversionManager(s3_bucket=None).start_conversion_process(dag_file, self.output_dir, user_validate=True)
        tasks = yaml.safe_load((self.output_dir / f"{self.dag_id}.yaml").read_text())[self.dag_id]["tasks"]

        self.assertNotIn("retries", tasks["inherits"])
        self.assertEqual(tasks["overrides"]["retries"], 5)


class TestDynamicTaskMapping(CustomOperatorTestCase):
    def test_custom_partial_arguments_are_kept(self):
        """R7: partial() arguments outside the schema were dropped"""
        original, converted, tasks = self.convert(
            'AddOperator.partial(task_id="lit", y=100, tag="L").expand(x=[1, 2, 3])\n'
        )

        self.assertEqual(tasks["lit"]["partial"], {"y": 100, "tag": "L"})
        self.assertEqual(converted.get_task("lit").partial_kwargs["y"], 100)
        self.assertEqual(converted.get_task("lit").partial_kwargs["tag"], "L")
        self.assertEqual(converted.get_task("lit").expand_input.value, original.get_task("lit").expand_input.value)

    def test_expand_kwargs_is_rejected(self):
        """R8: DagFactory only supports a dict in 'expand'"""
        for body in (
            'AddOperator.partial(task_id="kw").expand_kwargs([{"x": 1, "y": 2}])\n',
            'src = AddOperator(task_id="src", x=1)\nAddOperator.partial(task_id="kw").expand_kwargs(src.output)\n',
        ):
            with self.subTest(body=body), self.assertRaisesRegex(InvalidOperatorError, "expand_kwargs"):
                self.convert(body)


class TestUnsupportedDefinitions(CustomOperatorTestCase):
    def test_operator_defined_in_dag_file(self):
        """R1: classes defined in the Dag file only exist under DagBag's internal module name"""
        body = """
        from airflow.sdk import BaseOperator

        class InlineOperator(BaseOperator):
            def execute(self, context):
                return "hi"

        InlineOperator(task_id="inline")
        """
        with self.assertRaisesRegex(InvalidOperatorError, "defined in the Dag file"):
            self.convert(body)

    def test_functools_partial_value(self):
        """R11"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "functools.partial"):
            self.convert('PayloadOperator(task_id="p", payload=functools.partial(multiply, 2))\n')

    def test_list_of_callbacks(self):
        """R12: DagFactory accepts a single callback"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "on_success_callback"):
            self.convert('AddOperator(task_id="cb", x=1, on_success_callback=[record, record2])\n')

    def test_single_callback_in_list_is_unwrapped(self):
        _, converted, tasks = self.convert('AddOperator(task_id="cb", x=1, on_success_callback=[record])\n')

        self.assertEqual(tasks["cb"]["on_success_callback"], f"{self.ops}.record")
        self.assertEqual([cb.__name__ for cb in converted.get_task("cb").on_success_callback], ["record"])


class TestArgumentValues(CustomOperatorTestCase):
    def test_int_enum(self):
        """R13: IntEnum is an int and was dumped before the Enum branch"""
        original, converted, _ = self.convert('PayloadOperator(task_id="p", payload=Level.HIGH)\n')

        self.assertIs(converted.get_task("p").payload, original.get_task("p").payload)

    def test_dataclass(self):
        original, converted, _ = self.convert('PayloadOperator(task_id="p", payload=Point(1, y=5))\n')

        self.assertEqual(converted.get_task("p").payload, original.get_task("p").payload)

    def test_plain_object_is_rejected(self):
        """R14: __dict__ does not have to match the __init__ arguments"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "Plain"):
            self.convert('PayloadOperator(task_id="p", payload=Plain(1, b=5))\n')


class TestUnverifiedOperatorWarnings(CustomOperatorTestCase):
    def test_module_attribute_and_unusual_class_names(self):
        with (
            patch("dag_converter.conversion.tasks.print_warning") as task_warning,
            patch("dag_converter.static_validator.print_warning") as static_warning,
        ):
            self.convert('m.AddOperator(task_id="attr", x=1)\nGreeter(task_id="greet")\n')

        warnings = " ".join(str(c) for c in task_warning.call_args_list + static_warning.call_args_list)
        self.assertIn("AddOperator", warnings)
        self.assertIn("Greeter", warnings)


if __name__ == "__main__":
    unittest.main()
