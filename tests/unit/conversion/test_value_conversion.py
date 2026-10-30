"""Conversion of argument values, checked by loading the converted YAML with DagFactory."""

import unittest

import pendulum

from dag_converter.conversion.exceptions import InvalidOperatorParameter
from tests.unit.conversion.test_custom_operators import DagConversionTestCase


class TestArgumentValues(DagConversionTestCase):
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

    def test_str_enums_keep_their_type(self):
        """P1: user str enums became plain strings, Airflow's own stay plain values"""
        original, converted, dag_yaml = self.convert_dag(
            tasks="PayloadOperator(task_id='p', payload=Key.A)\n"
            "    AddOperator(task_id='t', x=1, trigger_rule=__import__('airflow.utils.trigger_rule', "
            "fromlist=['TriggerRule']).TriggerRule.ALL_DONE)"
        )

        self.assertIs(converted.get_task("p").payload, original.get_task("p").payload)
        self.assertEqual(dag_yaml["tasks"]["t"]["trigger_rule"], "all_done")

    def test_explicit_none_overrides_default_args(self):
        original, converted, tasks = self.convert_dag(
            "start_date=datetime(2026, 1, 1), default_args={'execution_timeout': timedelta(minutes=5)}",
            tasks="t = AddOperator(task_id='no_timeout', x=1, execution_timeout=None)",
        )

        self.assertIsNone(original.get_task("no_timeout").execution_timeout)
        self.assertIsNone(converted.get_task("no_timeout").execution_timeout)


class TestDatetimeValues(DagConversionTestCase):
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

    def test_pendulum_datetime_keeps_its_type(self):
        """P7"""
        original, converted, _ = self.convert_dag(
            tasks="PayloadOperator(task_id='p', payload=pendulum.datetime(2024, 5, 6, 7, 8, tz='Asia/Kolkata'))"
        )

        payload = converted.get_task("p").payload
        self.assertIsInstance(payload, pendulum.DateTime)
        self.assertEqual(payload, original.get_task("p").payload)
        self.assertEqual(payload.timezone_name, "Asia/Kolkata")

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


class TestCallableValues(DagConversionTestCase):
    def test_functools_partial_value(self):
        """R11"""
        with self.assertRaisesRegex(InvalidOperatorParameter, "functools.partial"):
            self.convert('PayloadOperator(task_id="p", payload=functools.partial(multiply, 2))\n')

    def test_lambdas_nested_functions_and_methods_are_rejected(self):
        """N10: DagFactory imports callables with a single attribute lookup"""
        for value in ("lambda context: None", "Helpers.triple"):
            with self.subTest(value=value), self.assertRaises(InvalidOperatorParameter):
                self.convert_dag(tasks=f"AddOperator(task_id='t', x=1, on_success_callback={value})")

    def test_callables_dagfactory_does_not_import(self):
        """P2: callables in other arguments, or nested in containers, would arrive as strings"""
        for value in (
            "PayloadOperator(task_id='p', payload=multiply)",
            "PayloadOperator(task_id='p', payload={'fn': multiply})",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(InvalidOperatorParameter, "multiply"):
                self.convert_dag(tasks=value)


if __name__ == "__main__":
    unittest.main()
