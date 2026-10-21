import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from dag_converter.conversion.taskflow_module import build_taskflow_module, get_taskflow_module_name


class TestGetTaskflowModuleName(unittest.TestCase):
    def test_dag_id_with_invalid_identifier_characters(self):
        """Dag ids may contain '-' and '.', which are not valid in module names"""
        self.assertEqual(get_taskflow_module_name("my-dag.v2"), "my_dag_v2_taskflow")

    def test_dag_id_starting_with_digit(self):
        self.assertEqual(get_taskflow_module_name("2024_dag"), "_2024_dag_taskflow")


class TestBuildTaskflowModule(unittest.TestCase):
    def test_no_decorator_tasks(self):
        tasks = {"bash": {"operator": "airflow.providers.standard.operators.bash.BashOperator"}}

        self.assertIsNone(build_taskflow_module("my_dag", tasks))
        self.assertNotIn("python_callable", tasks["bash"])

    def test_python_callables_point_at_companion_module(self):
        tasks = {
            "extract": {"decorator": "python_task", "python_callable": "my_dag_file.extract"},
            "load": {"decorator": "python_task", "python_callable": "my_dag_file.load"},
            "classic": {"operator": "PythonOperator", "python_callable": "my_dag_file.plain_function"},
        }

        module_name, source = build_taskflow_module("my_dag", tasks)

        self.assertEqual(module_name, "my_dag_taskflow")
        self.assertEqual(tasks["extract"]["python_callable"], "my_dag_taskflow.extract")
        self.assertEqual(tasks["load"]["python_callable"], "my_dag_taskflow.load")
        # Classic operators take the function directly
        self.assertEqual(tasks["classic"]["python_callable"], "my_dag_file.plain_function")
        self.assertIn("import my_dag_file\n", source)
        self.assertIn("extract = _undecorated(my_dag_file.extract)", source)

    def test_same_function_used_by_multiple_tasks(self):
        tasks = {
            "double": {"decorator": "python_task", "python_callable": "my_dag_file.double"},
            "double__1": {"decorator": "python_task", "python_callable": "my_dag_file.double"},
        }

        _, source = build_taskflow_module("my_dag", tasks)

        self.assertEqual(source.count("double = _undecorated(my_dag_file.double)"), 1)
        self.assertEqual(tasks["double__1"]["python_callable"], "my_dag_taskflow.double")

    def test_same_function_name_in_different_modules(self):
        tasks = {
            "a": {"decorator": "python_task", "python_callable": "my_dag_file.transform"},
            "b": {"decorator": "python_task", "python_callable": "helpers.tasks.transform"},
        }

        _, source = build_taskflow_module("my_dag", tasks)

        self.assertEqual(tasks["a"]["python_callable"], "my_dag_taskflow.transform")
        self.assertEqual(tasks["b"]["python_callable"], "my_dag_taskflow.helpers_tasks__transform")
        self.assertIn("import helpers.tasks\n", source)
        self.assertIn("helpers_tasks__transform = _undecorated(helpers.tasks.transform)", source)

    def test_generated_module_exposes_undecorated_functions(self):
        """The generated module must import and expose plain functions DagFactory can inspect"""
        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, "taskflow_source_dag.py").write_text(
                textwrap.dedent(
                    """
                    from airflow.sdk import task

                    @task
                    def add(x, y=1):
                        return x + y

                    def plain(value):
                        return value
                    """
                )
            )
            tasks = {
                "add": {"decorator": "python_task", "python_callable": "taskflow_source_dag.add"},
                "plain": {"decorator": "python_task", "python_callable": "taskflow_source_dag.plain"},
            }
            module_name, source = build_taskflow_module("source_dag", tasks)
            Path(temp_dir, f"{module_name}.py").write_text(source)

            sys.path.insert(0, temp_dir)
            try:
                module = __import__(module_name)
                self.assertEqual(module.add(2, y=3), 5)
                self.assertEqual(module.add.__name__, "add")
                self.assertEqual(module.plain("value"), "value")
            finally:
                sys.path.remove(temp_dir)
                sys.modules.pop(module_name, None)
                sys.modules.pop("taskflow_source_dag", None)


if __name__ == "__main__":
    unittest.main()
