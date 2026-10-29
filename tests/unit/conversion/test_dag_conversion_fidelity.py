"""Dag-level and value conversions, checked by loading the converted YAML with DagFactory."""

import unittest

from dag_converter.conversion.exceptions import InvalidDagParameter
from dag_converter.conversion_manager import ConversionManager
from tests.unit.conversion.test_custom_operators import CustomOperatorTestCase

DAG_FILE = """
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import PurePosixPath

import pendulum
from airflow import DAG
from airflow.sdk import Param

from {ops} import *

with DAG(dag_id="{dag_id}", schedule=None, catchup=False, {dag_kwargs}) as dag:
    {tasks}
"""


class DagConversionTestCase(CustomOperatorTestCase):
    def convert_dag(
        self, dag_kwargs: str = "start_date=datetime(2026, 1, 1)", tasks: str = 'AddOperator(task_id="t", x=1)'
    ):
        source = DAG_FILE.replace("{dag_kwargs}", dag_kwargs).replace("{tasks}", tasks)
        return self.convert_source(source)


class TestDagArguments(DagConversionTestCase):
    def test_dag_callbacks(self):
        """R6: typos in the schema dropped Dag-level callbacks"""
        _, converted, dag_yaml = self.convert_dag(
            "start_date=datetime(2026, 1, 1), on_success_callback=record, on_failure_callback=[record2]"
        )

        self.assertEqual(dag_yaml["on_success_callback"], f"{self.ops}.record")
        self.assertEqual(converted.on_success_callback.__name__, "record")
        self.assertEqual(converted.on_failure_callback.__name__, "record2")

    def test_arguments_dagfactory_drops_are_rejected(self):
        """R9: DagFactory 1.0 does not pass user_defined_filters and user_defined_macros to the Dag"""
        for kwargs in (
            "user_defined_filters={'shout': str.upper}",
            "user_defined_macros={'env': 'prod'}",
            "fail_fast=True",
        ):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(InvalidDagParameter, "not supported"):
                self.convert_dag(f"start_date=datetime(2026, 1, 1), {kwargs}")

    def test_param_schemas(self):
        """R10: Param schemas were flattened to their values"""
        original, converted, _ = self.convert_dag(
            "start_date=datetime(2026, 1, 1), params={'limit': Param(5, type='integer', minimum=1, "
            "description='Rows'), 'name': 'plain'}"
        )

        for key in ("limit", "name"):
            self.assertEqual(converted.params.get_param(key).dump(), original.params.get_param(key).dump())

    def test_default_args_with_operator_parameters(self):
        """R17: default_args were limited to an allowlist"""
        original, converted, dag_yaml = self.convert_dag(
            "start_date=datetime(2026, 1, 1), default_args={'pool': 'my_pool', 'queue': 'q', "
            "'retry_exponential_backoff': True, 'on_failure_callback': record}"
        )

        task = converted.get_task("t")
        self.assertEqual(task.pool, "my_pool")
        self.assertEqual(task.queue, "q")
        self.assertTrue(task.retry_exponential_backoff)
        self.assertEqual([cb.__name__ for cb in task.on_failure_callback], ["record"])

    def test_unknown_default_args_are_rejected(self):
        from dag_converter.conversion.exceptions import InvalidDefaultArg

        with self.assertRaisesRegex(InvalidDefaultArg, "not_an_argument"):
            self.convert_dag("start_date=datetime(2026, 1, 1), default_args={'not_an_argument': 1}")


class TestTimezones(DagConversionTestCase):
    def test_dag_start_date_timezone(self):
        """DagFactory replaces the tzinfo of start_date with the Dag 'timezone'"""
        original, converted, dag_yaml = self.convert_dag(
            "start_date=pendulum.datetime(2026, 1, 1, 6, tz='Europe/Berlin'), "
            "end_date=pendulum.datetime(2026, 6, 1, tz='Europe/Berlin'), "
            "default_args={'start_date': pendulum.datetime(2026, 1, 2, tz='Europe/Berlin')}"
        )

        self.assertEqual(dag_yaml["timezone"], "Europe/Berlin")
        self.assertEqual(converted.start_date, original.start_date)
        self.assertEqual(converted.end_date, original.end_date)
        self.assertEqual(converted.timezone.name, "Europe/Berlin")
        self.assertEqual(converted.get_task("t").start_date, original.get_task("t").start_date)

    def test_aware_task_arguments(self):
        """R16: aware datetimes became naive"""
        original, converted, _ = self.convert_dag(
            tasks="PayloadOperator(task_id='utc', payload=datetime(2026, 1, 1, tzinfo=timezone.utc))\n"
            "    PayloadOperator(task_id='berlin', payload=pendulum.datetime(2026, 1, 1, tz='Europe/Berlin'))\n"
            "    PayloadOperator(task_id='offset', payload=datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2))))\n"
        )

        for task_id in ("utc", "berlin", "offset"):
            self.assertEqual(converted.get_task(task_id).payload, original.get_task(task_id).payload)
            self.assertEqual(
                converted.get_task(task_id).payload.utcoffset(), original.get_task(task_id).payload.utcoffset()
            )


class TestValues(DagConversionTestCase):
    def test_value_types(self):
        """R15, R16: None in containers became 'None', several types became str or list"""
        values = {
            "nested_none": "{'a': [1, None]}",
            "date": "date(2026, 1, 2)",
            "time": "time(12, 30, 5)",
            "decimal": "Decimal('1.10')",
            "frozenset": "frozenset({'a', 'b'})",
            "set": "{1, 2}",
            "tuple": "(1, 'b')",
            "path": "PurePosixPath('/tmp/data.csv')",
        }
        tasks = "\n    ".join(f"PayloadOperator(task_id='{key}', payload={value})" for key, value in values.items())

        original, converted, _ = self.convert_dag(tasks=tasks)

        for task_id in values:
            with self.subTest(task_id=task_id):
                self.assertEqual(converted.get_task(task_id).payload, original.get_task(task_id).payload)
                self.assertIs(type(converted.get_task(task_id).payload), type(original.get_task(task_id).payload))


class TestConversionRuns(CustomOperatorTestCase):
    def _dag_file(self, name: str, dag_ids: list[str], failing: str | None = None) -> None:
        tasks = []
        for dag_id in dag_ids:
            task = "Plain(1)" if dag_id == failing else "1"
            tasks.append(
                f"with DAG(dag_id='{dag_id}', start_date=datetime(2026, 1, 1), schedule=None):\n"
                f"    PayloadOperator(task_id='t', payload={task})\n"
            )
        header = f"from datetime import datetime\nfrom airflow import DAG\nfrom {self.ops} import *\n\n"
        source = header + "\n".join(tasks)
        (self.input_dir / name).write_text(source)

    def setUp(self):
        super().setUp()
        self.input_dir = self.work_dir / "dags"
        self.input_dir.mkdir()
        # The operators module next to the Dag files is also a helper module the directory conversion skips
        (self.input_dir / f"{self.ops}.py").write_text((self.work_dir / f"{self.ops}.py").read_text())

    def test_same_dag_id_in_two_files(self):
        """R18: the second file overwrote the first one's YAML"""
        self._dag_file("first.py", [f"{self.dag_id}_shared"])
        self._dag_file("second.py", [f"{self.dag_id}_shared"])

        with self.assertRaisesRegex(Exception, "1 out of 2 files"):
            ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir, self.output_dir)

    def test_failing_dag_does_not_stop_the_file(self):
        """R19: one failing Dag aborted the remaining Dags of the file"""
        ok_dag, bad_dag = f"{self.dag_id}_ok", f"{self.dag_id}_bad"
        self._dag_file("both.py", [bad_dag, ok_dag], failing=bad_dag)

        with self.assertRaisesRegex(Exception, bad_dag):
            ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir / "both.py", self.output_dir)

        self.assertTrue((self.output_dir / f"{ok_dag}.yaml").exists())

    def test_helper_modules_in_directory_are_skipped(self):
        self._dag_file("dag.py", [f"{self.dag_id}_dir"])
        (self.input_dir / "helpers.py").write_text("def helper():\n    return 1\n")

        ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir, self.output_dir)

        self.assertTrue((self.output_dir / f"{self.dag_id}_dir.yaml").exists())

    def test_failure_message_has_counts(self):
        self._dag_file("bad.py", [f"{self.dag_id}_bad"], failing=f"{self.dag_id}_bad")

        with self.assertRaises(Exception) as context:
            ConversionManager(s3_bucket=None).start_conversion_process(self.input_dir, self.output_dir)

        self.assertIn("1 out of 1 files", str(context.exception))
        self.assertNotIn("{", str(context.exception))


if __name__ == "__main__":
    unittest.main()
