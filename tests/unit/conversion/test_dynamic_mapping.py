import unittest
from unittest.mock import Mock

from airflow.sdk.definitions._internal.expandinput import ListOfDictsExpandInput

from dag_converter.conversion.dynamic_mapping import handle_dynamic_task_mapping, reformat
from dag_converter.conversion.exceptions import InvalidOperatorError


class TestHandleDynamicTaskMapping(unittest.TestCase):
    def setUp(self):
        self.task_type = {"task1": "_PythonDecoratedOperator", "task2": "BashOperator"}

    def _mapped_task(self, partial_kwargs):
        task = Mock()
        task.task_id = "mapped"
        task.partial_kwargs = partial_kwargs
        del task.op_kwargs_expand_input
        del task.expand_input
        return task

    def test_op_kwargs_expand_input(self):
        """Test handling op_kwargs_expand_input"""

        task = Mock()
        task.op_kwargs_expand_input.value = {"param1": "value1"}
        task.partial_kwargs = {"python_callable": "func", "retries": 3}

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, {}, {})

        self.assertEqual(tasks_dict["expand"], {"param1": "value1"})

    def test_expand_input(self):
        """Test handling expand_input"""

        task = Mock()
        task.expand_input = Mock()
        task.expand_input.value = {"op_kwargs": {"param1": "value1"}}
        task.partial_kwargs = {"python_callable": "func", "retries": 3}
        del task.op_kwargs_expand_input

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, {}, {})

        self.assertEqual(tasks_dict["expand"], {"op_kwargs": {"param1": "value1"}})

    def test_expand_kwargs_is_rejected(self):
        task = Mock()
        task.task_id = "mapped"
        task.expand_input = ListOfDictsExpandInput([{"x": 1}])

        with self.assertRaisesRegex(InvalidOperatorError, "expand_kwargs"):
            handle_dynamic_task_mapping({}, self.task_type, task, {}, {})

    def test_partial_kwargs(self):
        """python_callable moves to the task, user arguments stay in partial"""
        task = self._mapped_task({"python_callable": "func", "retries": 3, "custom": "value"})

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, {"retries": 0}, {})

        self.assertEqual(tasks_dict["python_callable"], "func")
        self.assertEqual(tasks_dict["partial"], {"retries": 3, "custom": "value"})

    def test_partial_kwargs_defaults_are_dropped(self):
        """Airflow fills partial_kwargs with operator defaults and Dag default_args"""
        task = self._mapped_task(
            {
                "y": 100,
                "owner": "airflow",
                "retries": 2,
                "on_success_callback": [],
                "map_index_template": None,
                "execution_timeout": None,
                "dag": Mock(),
            }
        )

        tasks_dict = {"operator": "AddOperator"}

        handle_dynamic_task_mapping(
            tasks_dict, self.task_type, task, {"owner": "airflow", "on_success_callback": None}, {"retries": 2}
        )

        self.assertEqual(tasks_dict["partial"], {"y": 100})

    def test_partial_omitted_when_only_defaults(self):
        task = self._mapped_task({"owner": "airflow"})

        tasks_dict = {}

        handle_dynamic_task_mapping(tasks_dict, self.task_type, task, {"owner": "airflow"}, {})

        self.assertNotIn("partial", tasks_dict)


class TestReformat(unittest.TestCase):
    def setUp(self):
        self.task_type = {"task1": "_PythonDecoratedOperator", "task2": "BashOperator"}

    def test_reformat_dict_python_decorated(self):
        """Test reformatting dict with PythonDecoratedOperator"""
        expand_dict = {"param": "task_instance.xcom_pull(task_ids='task1')"}

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict["param"], "+task1")

    def test_reformat_dict_regular_operator(self):
        """Test reformatting dict with regular operator"""
        expand_dict = {"param": "task_instance.xcom_pull(task_ids='task2')"}

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict["param"], "task2.output")

    def test_reformat_list(self):
        """Test reformatting list"""
        expand_dict = ["task_instance.xcom_pull(task_ids='task1')", "task_instance.xcom_pull(task_ids='task2')"]

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict[0], "+task1")
        self.assertEqual(expand_dict[1], "task2.output")

    def test_reformat_no_xcom(self):
        """Test reformatting with no xcom references"""
        expand_dict = {"param": "regular_value"}

        reformat(expand_dict, self.task_type)

        self.assertEqual(expand_dict["param"], "regular_value")

    def test_reformat_no_task_id_match(self):
        """Test reformatting when task_id regex doesn't match"""
        expand_dict = {"param": "task_instance.xcom_pull(invalid_format)"}

        # This should raise a KeyError because task_id is None
        with self.assertRaises(KeyError):
            reformat(expand_dict, self.task_type)


if __name__ == "__main__":
    unittest.main()
