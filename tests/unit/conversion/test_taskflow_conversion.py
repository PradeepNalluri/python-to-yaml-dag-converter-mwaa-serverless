"""Conversion of TaskFlow (@task) tasks into DagFactory decorator tasks."""

import shutil
import tempfile
import textwrap
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import yaml
from airflow.models.dag import DAG
from airflow.sdk import task

from dag_converter.cleanup import get_cleanup_dag
from dag_converter.conversion.tasks import convert_tasks
from dag_converter.conversion_manager import ConversionManager
from dag_converter.schema_parser import ArgumentValidator

SCHEMA_FILE = Path(__file__).parent.parent.parent.parent / "dag_converter" / "validator_schema.yaml"
DECORATORS = "airflow.providers.standard.decorators"


@task
def extract():
    return [1, 2, 3]


@task(retries=3)
def transform(numbers, factor, offset=0):
    return [n * factor + offset for n in numbers]


@task.branch
def choose():
    return "extract"


@task.short_circuit
def gate():
    return True


@task.bash
def echo(message):
    return f"echo {message}"


@task
def double(number):
    return number * 2


class TestTaskflowConversion(unittest.TestCase):
    def setUp(self):
        self.validator = ArgumentValidator(SCHEMA_FILE)

    def _convert(self, dag):
        return get_cleanup_dag(convert_tasks(Mock(), dag, Path("test.py"), self.validator))

    def test_decorator_import_paths(self):
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            extract()
            choose()
            gate()
            echo("hi")

        tasks = self._convert(dag)

        self.assertEqual(tasks["extract"]["decorator"], f"{DECORATORS}.python.python_task")
        self.assertEqual(tasks["choose"]["decorator"], f"{DECORATORS}.branch_python.branch_task")
        self.assertEqual(tasks["gate"]["decorator"], f"{DECORATORS}.short_circuit.short_circuit_task")
        self.assertEqual(tasks["echo"]["decorator"], f"{DECORATORS}.bash.bash_task")
        for converted in tasks.values():
            self.assertNotIn("operator", converted)

    def test_call_arguments_become_task_keys(self):
        """Positional and keyword arguments are named by the function signature, upstream outputs use '+'"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            transform(extract(), 10, offset=1)

        tasks = self._convert(dag)

        self.assertEqual(tasks["transform"]["numbers"], "+extract")
        self.assertEqual(tasks["transform"]["factor"], 10)
        self.assertEqual(tasks["transform"]["offset"], 1)
        self.assertEqual(tasks["transform"]["retries"], 3)
        self.assertEqual(tasks["transform"]["dependencies"], ["extract"])
        for internal in ("op_args", "op_kwargs", "kwargs_to_upstream", "dag", "task_group"):
            self.assertNotIn(internal, tasks["transform"])

    def test_dynamic_task_mapping(self):
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            double.expand(number=extract())

        tasks = self._convert(dag)

        self.assertEqual(tasks["double"]["decorator"], f"{DECORATORS}.python.python_task")
        self.assertEqual(tasks["double"]["expand"], {"number": "+extract"})


class TestTaskflowConversionEndToEnd(unittest.TestCase):
    """Converts a TaskFlow Dag file and validates the output with DagFactory"""

    def setUp(self):
        self.input_dir = Path(tempfile.mkdtemp())
        self.output_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.input_dir)
        shutil.rmtree(self.output_dir)

    def test_convert_and_validate(self):
        dag_file = self.input_dir / "e2e_taskflow_dag.py"
        dag_file.write_text(
            textwrap.dedent(
                """
                from datetime import datetime

                from airflow.sdk import dag, task


                @task
                def get_number():
                    return 5


                @task
                def add(number, amount):
                    return number + amount


                @dag(dag_id="e2e-taskflow", start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
                def e2e_taskflow():
                    add(get_number(), amount=2)


                e2e_taskflow()
                """
            )
        )

        ConversionManager(s3_bucket=None).start_conversion_process(
            dag_file_path=dag_file, output_dir=self.output_dir, user_validate=True
        )

        converted = yaml.safe_load((self.output_dir / "e2e-taskflow.yaml").read_text())["e2e-taskflow"]["tasks"]
        self.assertEqual(converted["add"]["python_callable"], "e2e_taskflow_taskflow.add")
        self.assertEqual(converted["add"]["number"], "+get_number")
        self.assertEqual(converted["add"]["amount"], 2)
        module_source = (self.output_dir / "e2e_taskflow_taskflow.py").read_text()
        self.assertIn("add = _undecorated(e2e_taskflow_dag.add)", module_source)


if __name__ == "__main__":
    unittest.main()
