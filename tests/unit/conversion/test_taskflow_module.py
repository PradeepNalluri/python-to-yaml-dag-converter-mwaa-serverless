import importlib
import shutil
import sys
import tempfile
import textwrap
import unittest
import uuid
from pathlib import Path

from airflow.models.dagbag import DagBag

from dag_converter.conversion.exceptions import InvalidOperatorError
from dag_converter.conversion.taskflow_module import build_taskflow_module, get_taskflow_module_name


class TestGetTaskflowModuleName(unittest.TestCase):
    def test_dag_id_with_invalid_identifier_characters(self):
        """Dag ids may contain '-' and '.', which are not valid in module names"""
        self.assertEqual(get_taskflow_module_name("my-dag.v2"), "my_dag_v2_taskflow")

    def test_dag_id_starting_with_digit(self):
        self.assertEqual(get_taskflow_module_name("2024_dag"), "_2024_dag_taskflow")


class TestBuildTaskflowModule(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        # Unique module names, modules from earlier tests stay in sys.modules
        self.suffix = uuid.uuid4().hex[:8]
        sys.path.insert(0, str(self.temp_dir))

    def tearDown(self):
        sys.path.remove(str(self.temp_dir))
        for name in [m for m in sys.modules if self.suffix in m]:
            del sys.modules[name]
        shutil.rmtree(self.temp_dir)

    def _write(self, name: str, source: str) -> Path:
        path = self.temp_dir / f"{name}_{self.suffix}.py"
        path.write_text(textwrap.dedent(source).replace("SUFFIX", self.suffix))
        return path

    def _build(self, dag_file: Path, dag_id: str = "test_dag"):
        """Load the Dag, build the module for its TaskFlow tasks and import it"""
        dag = DagBag(dag_folder=dag_file, include_examples=False).dags[dag_id]
        tasks = {
            t.task_id: {"decorator": "python_task"} for t in dag.tasks if t.task_type.endswith("DecoratedOperator")
        }
        tasks.update({t.task_id: {"operator": t.task_type} for t in dag.tasks if t.task_id not in tasks})
        module_name, source = build_taskflow_module(dag, dag_file, tasks)
        (self.temp_dir / f"{module_name}.py").write_text(source)
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()
        module = importlib.import_module(module_name)
        return tasks, source, module

    def test_no_decorator_tasks(self):
        dag_file = self._write(
            "classic_dag",
            """
            from datetime import datetime
            from airflow.sdk import DAG
            from airflow.providers.standard.operators.empty import EmptyOperator

            with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None):
                EmptyOperator(task_id="noop")
            """,
        )
        dag = DagBag(dag_folder=dag_file, include_examples=False).dags["test_dag"]

        self.assertIsNone(build_taskflow_module(dag, dag_file, {"noop": {"operator": "EmptyOperator"}}))

    def test_functions_are_copied_without_importing_the_dag_file(self):
        """Importing the Dag file would create the original Dag next to the converted one"""
        dag_file = self._write(
            "taskflow_dag",
            """
            import json
            import os.path
            from datetime import datetime
            from os.path import join as join_path

            from airflow.sdk import dag, task

            SEPARATOR = "-"
            LIMITS = {"max": 3}


            def helper(value):
                return f"{value}{SEPARATOR}"


            class Formatter:
                prefix = SEPARATOR

                def format(self, value):
                    return json.dumps([self.prefix, value])


            @task
            def module_level(value, limit=LIMITS["max"]):
                return helper(value) * limit


            @dag(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None)
            def test_dag():
                suffix = "!"

                @task(task_id="nested_task")
                def nested(value):
                    return join_path(os.path.sep, Formatter().format(value) + suffix)

                nested(module_level("a"))


            test_dag()
            """,
        )

        tasks, source, module = self._build(dag_file)

        self.assertNotIn(f"taskflow_dag_{self.suffix}", source)
        self.assertNotIn("@task", source)
        self.assertEqual(tasks["module_level"]["python_callable"], "test_dag_taskflow.module_level")
        self.assertEqual(tasks["nested_task"]["python_callable"], "test_dag_taskflow.nested")
        self.assertIn("from os.path import join as join_path", source)
        self.assertIn("import os.path", source)
        self.assertEqual(module.module_level("a"), "a-a-a-")
        self.assertEqual(module.nested("b"), '/["-", "b"]!')

    def test_function_from_another_module_is_imported(self):
        self._write(
            "helpers",
            """
            from airflow.sdk import task

            @task
            def shared(value):
                return value * 2
            """,
        )
        dag_file = self._write(
            "imports_task_dag",
            """
            from datetime import datetime
            from airflow.sdk import dag
            from helpers_SUFFIX import shared

            @dag(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None)
            def test_dag():
                shared(2)

            test_dag()
            """,
        )

        tasks, source, module = self._build(dag_file)

        self.assertIn(f"shared = _undecorated(helpers_{self.suffix}.shared)", source)
        self.assertEqual(module.shared(2), 4)

    def test_closure_over_non_constant_is_unsupported(self):
        dag_file = self._write(
            "closure_dag",
            """
            from datetime import datetime
            from airflow.sdk import dag, task

            @dag(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None)
            def test_dag():
                client = object()

                @task
                def uses_client():
                    return client

                uses_client()

            test_dag()
            """,
        )
        dag = DagBag(dag_folder=dag_file, include_examples=False).dags["test_dag"]

        with self.assertRaises(InvalidOperatorError) as context:
            build_taskflow_module(dag, dag_file, {"uses_client": {"decorator": "python_task"}})
        self.assertIn("'client'", str(context.exception))

    def test_module_level_object_is_unsupported(self):
        dag_file = self._write(
            "object_dag",
            """
            from datetime import datetime
            from airflow.sdk import dag, task

            CLIENT = object()

            @task
            def uses_client():
                return CLIENT

            @dag(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None)
            def test_dag():
                uses_client()

            test_dag()
            """,
        )
        dag = DagBag(dag_folder=dag_file, include_examples=False).dags["test_dag"]

        with self.assertRaises(InvalidOperatorError) as context:
            build_taskflow_module(dag, dag_file, {"uses_client": {"decorator": "python_task"}})
        self.assertIn("'CLIENT'", str(context.exception))

    def test_same_function_used_by_multiple_tasks(self):
        dag_file = self._write(
            "reused_dag",
            """
            from datetime import datetime
            from airflow.sdk import dag, task

            @task
            def double(number):
                return number * 2

            @dag(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None)
            def test_dag():
                double(1)
                double(2)

            test_dag()
            """,
        )

        tasks, source, module = self._build(dag_file)

        self.assertEqual(tasks["double"]["python_callable"], "test_dag_taskflow.double")
        self.assertEqual(tasks["double__1"]["python_callable"], "test_dag_taskflow.double")
        self.assertEqual(source.count("def double("), 1)


if __name__ == "__main__":
    unittest.main()
