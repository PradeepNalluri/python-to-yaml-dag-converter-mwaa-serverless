"""Conversions found by the second custom operator test round (N1-N10)."""

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import pendulum

from dag_converter.conversion.exceptions import InvalidDagParameter, InvalidOperatorParameter
from tests.unit.conversion.test_dag_conversion_fidelity import DagConversionTestCase


class TestDefaultArgs(DagConversionTestCase):
    def test_task_group_default_args(self):
        """N1: TaskGroup default_args were dropped"""
        original, converted, _ = self.convert_dag(
            "start_date=datetime(2026, 1, 1), default_args={'retries': 1}",
            tasks="from airflow.sdk import TaskGroup\n"
            "    with TaskGroup('g', default_args={'retries': 4, 'owner': 'grp', 'trigger_rule': 'all_done'}):\n"
            "        with TaskGroup('inner', default_args={'owner': 'inner'}):\n"
            "            AddOperator(task_id='t', x=1)\n"
            "        AddOperator(task_id='own', x=1, retries=7)\n",
        )

        for task_id in ("g.inner.t", "g.own"):
            a, b = original.get_task(task_id), converted.get_task(task_id)
            for attribute in ("retries", "owner", "trigger_rule"):
                self.assertEqual(getattr(b, attribute), getattr(a, attribute), f"{task_id}.{attribute}")

    def test_partial_values_equal_to_operator_default_but_not_default_args(self):
        """N2: the effective default of a mapped task is the default_args value"""
        original, converted, _ = self.convert_dag(
            "start_date=datetime(2026, 1, 1), default_args={'retries': 2, 'trigger_rule': 'all_done'}",
            tasks="AddOperator.partial(task_id='dflt', retries=0, trigger_rule='all_success', tag='D')"
            ".expand(x=[1, 2])",
        )

        partial = converted.get_task("dflt").partial_kwargs
        self.assertEqual(partial["retries"], 0)
        self.assertEqual(partial["trigger_rule"], "all_success")

    def test_operator_added_to_dag_after_creation(self):
        """Airflow did not apply default_args to it, DagFactory does"""
        original, converted, _ = self.convert_source(
            """
            from datetime import datetime
            from airflow import DAG
            from {ops} import *

            late = AddOperator(task_id="late", x=1)
            with DAG(dag_id="{dag_id}", start_date=datetime(2026, 1, 1), schedule=None,
                     default_args={"retries": 2, "owner": "late"}) as dag:
                AddOperator(task_id="inside", x=1)
            dag.add_task(late)
            """
        )

        for task_id in ("late", "inside"):
            for attribute in ("retries", "owner"):
                self.assertEqual(
                    getattr(converted.get_task(task_id), attribute), getattr(original.get_task(task_id), attribute)
                )

    def test_explicit_none_overrides_default_args(self):
        original, converted, tasks = self.convert_dag(
            "start_date=datetime(2026, 1, 1), default_args={'execution_timeout': timedelta(minutes=5)}",
            tasks="t = AddOperator(task_id='no_timeout', x=1, execution_timeout=None)",
        )

        self.assertIsNone(original.get_task("no_timeout").execution_timeout)
        self.assertIsNone(converted.get_task("no_timeout").execution_timeout)


class TestAssignedAttributes(DagConversionTestCase):
    def test_kwargs_only_attribute_assigned_after_creation(self):
        """N6: 'extra' is consumed through **kwargs, so it is not a constructor parameter name"""
        original, converted, tasks = self.convert_dag(
            tasks="kw = KwargsOperator(task_id='kw_attr', foo='F', extra=1)\n    kw.extra = 99"
        )

        self.assertEqual(converted.get_task("kw_attr").extra, 99)

    def test_attribute_the_operator_does_not_define(self):
        """The operator does not read it, e.g. a deprecated attribute name, so it is ignored with a warning"""
        with patch("dag_converter.conversion.tasks.print_warning") as warning:
            _, _, dag_yaml = self.convert_dag(tasks="t = AddOperator(task_id='t', x=1)\n    t.not_an_argument = 5")

        self.assertNotIn("not_an_argument", dag_yaml["tasks"]["t"])
        self.assertIn("not_an_argument", str(warning.call_args_list))

    def test_attribute_init_derived_is_rejected(self):
        """'tag' is an argument, but 'x' and 'y' are also stored; an attribute only __init__ sets cannot be passed"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "derived"):
            self.convert_dag(tasks="t = DerivedOperator(task_id='t', value=1)\n    t.derived = 5")

    def test_operator_updating_its_own_attributes(self):
        """GlueJobOperator sets s3_script_location in its own methods, which is not an assignment"""
        _, converted, _ = self.convert_dag(tasks="t = SelfUpdatingOperator(task_id='t')\n    t.refresh()")

        self.assertEqual(converted.get_task("t").state, "initial")

    def test_container_argument_changed_in_place(self):
        original, converted, _ = self.convert_dag(
            tasks="t = PayloadOperator(task_id='t', payload={'a': 1})\n    t.payload['b'] = 2"
        )

        self.assertEqual(converted.get_task("t").payload, {"a": 1, "b": 2})

    def test_none_normalized_to_empty_container_is_not_a_transformation(self):
        _, converted, _ = self.convert_dag(
            tasks="t = NormalizingOperator(task_id='t')\n    t.options = {'k': 'v'}"
        )

        self.assertEqual(converted.get_task("t").options, {"k": "v"})

    def test_transformed_attribute_assigned_after_creation(self):
        """N7: __init__ would transform the assigned value again"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "prefix"):
            self.convert_dag(tasks="pre = PrefixOperator(task_id='prefix_attr')\n    pre.prefix = 's3://other/'")


class TestValues(DagConversionTestCase):
    def test_task_param_schema(self):
        """N3: task-level Param was rejected"""
        original, converted, _ = self.convert_dag(
            tasks="AddOperator(task_id='t', x=1, params={'tp': Param(3, type='integer', minimum=2)})"
        )

        self.assertEqual(
            converted.get_task("t").params.get_param("tp").dump(), original.get_task("t").params.get_param("tp").dump()
        )

    def test_objects_without_dict_are_rejected(self):
        """N4: they were written as str"""
        for value in ("__import__('uuid').UUID(int=1)", "range(3)", "complex(1, 2)", "Slotted(1)"):
            with self.subTest(value=value), self.assertRaises(InvalidOperatorParameter):
                self.convert_dag(tasks=f"PayloadOperator(task_id='p', payload={value})")

    def test_namedtuple_enum_with_tuple_value_and_enum_keys(self):
        """N5"""
        original, converted, _ = self.convert_dag(
            tasks="PayloadOperator(task_id='named', payload=Pair(1, 2))\n"
            "    PayloadOperator(task_id='shape', payload=Shape.SQUARE)\n"
            "    PayloadOperator(task_id='path', payload=PurePosixPath('/tmp/x'))"
        )

        for task_id in ("named", "shape", "path"):
            self.assertEqual(converted.get_task(task_id).payload, original.get_task(task_id).payload)
            self.assertIs(type(converted.get_task(task_id).payload), type(original.get_task(task_id).payload))

    def test_unsupported_dict_keys(self):
        for value in ("{(1, 2): 'tuple key'}", "{'__type__': 'os.system'}", "{Key.A: 1}"):
            with self.subTest(value=value), self.assertRaises(InvalidOperatorParameter):
                self.convert_dag(tasks=f"PayloadOperator(task_id='p', payload={value})")

    def test_path_type_is_importable_on_all_python_versions(self):
        _, _, dag_yaml = self.convert_dag(tasks="PayloadOperator(task_id='p', payload=PurePosixPath('/tmp/x'))")

        self.assertEqual(dag_yaml["tasks"]["p"]["payload"]["__type__"], "pathlib.PurePosixPath")

    def test_lambdas_nested_functions_and_methods_are_rejected(self):
        """N10: DagFactory imports callables with a single attribute lookup"""
        for value in ("lambda context: None", "Helpers.triple"):
            with self.subTest(value=value), self.assertRaises(InvalidOperatorParameter):
                self.convert_dag(tasks=f"AddOperator(task_id='t', x=1, on_success_callback={value})")


class TestValidationRound1(DagConversionTestCase):
    def test_str_enums_keep_their_type(self):
        """P1: user str enums became plain strings, Airflow's own stay plain values"""
        original, converted, dag_yaml = self.convert_dag(
            tasks="PayloadOperator(task_id='p', payload=Key.A)\n"
            "    AddOperator(task_id='t', x=1, trigger_rule=__import__('airflow.utils.trigger_rule', "
            "fromlist=['TriggerRule']).TriggerRule.ALL_DONE)"
        )

        self.assertIs(converted.get_task("p").payload, original.get_task("p").payload)
        self.assertEqual(dag_yaml["tasks"]["t"]["trigger_rule"], "all_done")

    def test_callables_dagfactory_does_not_import(self):
        """P2: callables in other arguments, or nested in containers, would arrive as strings"""
        for value in (
            "PayloadOperator(task_id='p', payload=multiply)",
            "PayloadOperator(task_id='p', payload={'fn': multiply})",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(InvalidOperatorParameter, "multiply"):
                self.convert_dag(tasks=value)

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

    def test_kwargs_only_attribute_not_passed_to_constructor(self):
        """P6: KwargsOperator reads 'extra' from **kwargs, also when it was not passed"""
        _, converted, _ = self.convert_dag(tasks="kw = KwargsOperator(task_id='kw', foo='F')\n    kw.extra = 99")

        self.assertEqual(converted.get_task("kw").extra, 99)

    def test_pendulum_datetime_keeps_its_type(self):
        """P7"""
        original, converted, _ = self.convert_dag(
            tasks="PayloadOperator(task_id='p', payload=pendulum.datetime(2024, 5, 6, 7, 8, tz='Asia/Kolkata'))"
        )

        payload = converted.get_task("p").payload
        self.assertIsInstance(payload, pendulum.DateTime)
        self.assertEqual(payload, original.get_task("p").payload)
        self.assertEqual(payload.timezone_name, "Asia/Kolkata")


class TestValidationRound2(DagConversionTestCase):
    def test_two_callbacks_in_partial_are_rejected(self):
        """N1: callbacks moved out of 'partial' skipped the single callback check"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "single callback"):
            self.convert_dag(
                tasks="AddOperator.partial(task_id='m', on_success_callback=[record, record2]).expand(x=[1])"
            )

    def test_pendulum_types(self):
        """N2: naive pendulum DateTime, Date and Duration became datetime types"""
        values = {
            "naive": "pendulum.naive(2024, 1, 2, 3)",
            "date": "pendulum.date(2024, 1, 2)",
            "duration": "pendulum.duration(hours=2, minutes=5)",
            # N3: months and years are calendar units, intervals keep their start and end
            "months": "pendulum.duration(months=1, days=2)",
            "years": "pendulum.duration(years=1, hours=3, microseconds=5)",
        }
        tasks = "\n    ".join(f"PayloadOperator(task_id='{key}', payload={value})" for key, value in values.items())

        original, converted, _ = self.convert_dag(tasks=tasks)

        for task_id in values:
            a, b = original.get_task(task_id).payload, converted.get_task(task_id).payload
            self.assertEqual(b, a)
            self.assertIs(type(b), type(a))
            if task_id == "naive":
                self.assertEqual(b.fold, a.fold)
            if task_id in ("months", "years"):
                self.assertEqual((b.years, b.months, b.remaining_days), (a.years, a.months, a.remaining_days))


    def test_pendulum_interval_is_rejected(self):
        """DagFactory deep-copies its configuration, which fails for Interval"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "Interval"):
            self.convert_dag(
                tasks="PayloadOperator(task_id='p', payload=pendulum.interval("
                "pendulum.datetime(2024, 1, 1), pendulum.datetime(2024, 3, 1)))"
            )


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

    def test_start_date_in_repeated_dst_hour(self):
        """N9: the fold of 02:30 on the day the clocks go back was lost"""
        original, converted, _ = self.convert_dag(
            "start_date=datetime(2025, 10, 26, 2, 30, fold=1, tzinfo=__import__('zoneinfo').ZoneInfo('Europe/Berlin'))"
        )

        self.assertEqual(original.start_date, datetime(2025, 10, 26, 1, 30, tzinfo=timezone.utc))
        self.assertEqual(converted.start_date, original.start_date)


class TestDagArguments(DagConversionTestCase):
    def test_disable_bundle_versioning(self):
        """N10: dropped without an error"""
        for kwargs, name in (
            ("disable_bundle_versioning=True", "disable_bundle_versioning"),
            ("auto_register=False", "auto_register"),
        ):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(InvalidDagParameter, name):
                self.convert_dag(f"start_date=datetime(2026, 1, 1), {kwargs}")


if __name__ == "__main__":
    unittest.main()
