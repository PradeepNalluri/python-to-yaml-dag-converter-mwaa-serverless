"""Dag-level conversions, checked by loading the converted YAML with DagFactory."""

import unittest
from datetime import datetime, timezone

from dag_converter.conversion.exceptions import InvalidDagParameter
from tests.unit.conversion.test_custom_operators import DagConversionTestCase


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

    def test_disable_bundle_versioning_with_sdk_dag(self):
        """P3: airflow.sdk.DAG is not a subclass of airflow.models.dag.DAG"""
        source = """
            from datetime import datetime
            from airflow.sdk import DAG
            from {ops} import *

            with DAG(dag_id="{dag_id}", start_date=datetime(2026, 1, 1), schedule=None, disable_bundle_versioning=True):
                AddOperator(task_id="t", x=1)
            """
        with self.assertRaisesRegex(InvalidDagParameter, "disable_bundle_versioning"):
            self.convert_source(source)

    def test_disable_bundle_versioning(self):
        """N10: dropped without an error"""
        for kwargs, name in (
            ("disable_bundle_versioning=True", "disable_bundle_versioning"),
            ("auto_register=False", "auto_register"),
        ):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(InvalidDagParameter, name):
                self.convert_dag(f"start_date=datetime(2026, 1, 1), {kwargs}")


class TestDefaultArgCallbacks(DagConversionTestCase):
    def test_mapped_task_callbacks(self):
        """P4: callbacks were written in 'partial', which DagFactory does not import"""
        original, converted, dag_yaml = self.convert_dag(
            "start_date=datetime(2026, 1, 1), default_args={'on_failure_callback': record}",
            tasks="AddOperator.partial(task_id='m', on_success_callback=record2).expand(x=[1])",
        )

        task = dag_yaml["tasks"]["m"]
        self.assertNotIn("on_failure_callback", task.get("partial", {}))
        self.assertEqual(task["on_success_callback"], f"{self.ops}.record2")
        callbacks = converted.get_task("m").partial_kwargs["on_success_callback"]
        self.assertEqual([cb.__name__ for cb in callbacks], ["record2"])

    def test_inherited_callbacks(self):
        """P5: inherited callback lists failed DagFactory validation"""
        original, converted, _ = self.convert_source(
            """
            from datetime import datetime
            from airflow import DAG
            from airflow.sdk import TaskGroup
            from {ops} import *

            late = AddOperator(task_id="late", x=1)
            with DAG(dag_id="{dag_id}", start_date=datetime(2026, 1, 1), schedule=None,
                     default_args={"on_success_callback": record, "on_failure_callback": [record2]}) as dag:
                with TaskGroup("g", default_args={"on_success_callback": [record2]}):
                    AddOperator(task_id="t", x=1)
            dag.add_task(late)
            """
        )

        self.assertEqual([cb.__name__ for cb in converted.get_task("g.t").on_success_callback], ["record2"])
        self.assertFalse(converted.get_task("late").on_success_callback)
        self.assertFalse(converted.get_task("late").on_failure_callback)


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

    def test_start_date_in_repeated_dst_hour(self):
        """N9: the fold of 02:30 on the day the clocks go back was lost"""
        original, converted, _ = self.convert_dag(
            "start_date=datetime(2025, 10, 26, 2, 30, fold=1, tzinfo=__import__('zoneinfo').ZoneInfo('Europe/Berlin'))"
        )

        self.assertEqual(original.start_date, datetime(2025, 10, 26, 1, 30, tzinfo=timezone.utc))
        self.assertEqual(converted.start_date, original.start_date)


class TestSchedules(DagConversionTestCase):
    def test_cron_trigger_timetable_timezone(self):
        """N8: the timetable timezone was dropped"""
        source = self._timetable_dag("CronTriggerTimetable('0 9 * * *', timezone='America/New_York')")
        original, converted, _ = self.convert_source(source)

        self.assertEqual(converted.timetable.serialize(), original.timetable.serialize())

    def test_cron_data_interval_timetable(self):
        source = self._timetable_dag("CronDataIntervalTimetable('0 9 * * *', timezone='Europe/Berlin')")
        original, converted, _ = self.convert_source(source)

        self.assertEqual(converted.timetable.serialize(), original.timetable.serialize())

    def test_plain_cron_stays_a_string(self):
        _, _, dag_yaml = self.convert_source(self._timetable_dag("'0 9 * * *'"))

        self.assertEqual(dag_yaml["schedule"], "0 9 * * *")

    def _timetable_dag(self, schedule):
        return f"""
            from datetime import datetime
            from airflow import DAG
            from airflow.timetables.interval import CronDataIntervalTimetable
            from airflow.timetables.trigger import CronTriggerTimetable
            from {{ops}} import *

            with DAG(dag_id="{{dag_id}}", start_date=datetime(2026, 1, 1), schedule={schedule}, catchup=False):
                AddOperator(task_id="t", x=1)
            """


if __name__ == "__main__":
    unittest.main()
