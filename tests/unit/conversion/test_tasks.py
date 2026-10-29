import unittest
from unittest.mock import Mock, patch

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.mappedoperator import MappedOperator

from dag_converter.conversion.tasks import convert_tasks
from dag_converter.dagbag import get_constructor_parameter_names


class TestConvertTasks(unittest.TestCase):
    def setUp(self):
        self.mock_validator = Mock()
        self.mock_taskflow_parser = Mock()
        self.mock_dag_object = Mock()
        self.mock_dag_object.params = {}

    def test_basic_task_conversion(self):
        """Test basic task conversion"""
        # Setup mocks
        self.mock_validator.validate_field.return_value = True

        # Create mock task
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "BashOperator"
        task.__module__ = "airflow.operators.bash"
        task.upstream_task_ids = ["upstream_task"]
        task._is_mapped = False
        task.param1 = "value1"
        task.retries = 3
        task._BaseOperator__init_kwargs = {"param1": "value1", "retries": 3}
        # Ensure _task_module doesn't exist
        del task._task_module

        # Mock dir() to return our attributes
        with patch("builtins.dir", return_value=["task_id", "task_type", "param1", "retries"]):
            self.mock_dag_object.tasks = [task]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        expected = {
            "test_task": {
                "operator": "airflow.operators.bash.BashOperator",
                "param1": "value1",
                "retries": 3,
                "dependencies": ["upstream_task"],
            }
        }
        self.assertEqual(result, expected)

    @patch("dag_converter.conversion.tasks.get_operator_defaults", return_value={})
    @patch("dag_converter.conversion.tasks.handle_dynamic_task_mapping")
    def test_dynamic_task_mapping(self, mock_handle_dtm, mock_defaults):
        """Test dynamic task mapping handling"""
        self.mock_validator.validate_field.return_value = True

        task = Mock(spec=MappedOperator)
        task.task_id = "mapped_task"
        task.task_type = "BashOperator"
        task._task_module = "airflow.operators.bash"
        task.upstream_task_ids = []
        task._is_mapped = True
        task.param1 = "value1"
        task._BaseOperator__init_kwargs = {}

        with patch("builtins.dir", return_value=["task_id", "task_type", "param1"]):
            self.mock_dag_object.tasks = [task]

            convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        mock_handle_dtm.assert_called()

    @patch("dag_converter.conversion.tasks.get_operator_defaults", return_value={})
    def test_task_module_handling(self, mock_defaults):
        """Test _task_module attribute handling"""
        self.mock_validator.validate_field.return_value = True

        task = Mock(spec=MappedOperator)
        task.task_id = "dtm_task"
        task.task_type = "BashOperator"
        task._task_module = "airflow.operators.bash"
        task.__module__ = "different.module"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.param1 = "value1"
        task.expand_input.value = {"op_kwargs": {"param1": "value1"}}
        task.partial_kwargs = {"python_callable": "func", "retries": 3}
        task._BaseOperator__init_kwargs = {}

        with patch("builtins.dir", return_value=["task_id", "task_type", "param1"]):
            self.mock_dag_object.tasks = [task]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        self.assertEqual(result["dtm_task"]["operator"], "airflow.operators.bash.BashOperator")

    def test_special_operator_cleanup(self):
        """Test special operator field cleanup"""

        def validate_side_effect(section, key, value):
            return key in ["flow_update", "config"]

        self.mock_validator.validate_field.side_effect = validate_side_effect

        # Test AppFlow operator
        task1 = Mock()
        task1.task_id = "appflow_task"
        task1.task_type = "AppFlowOperator"
        task1.__module__ = "airflow.providers.amazon.aws.operators.appflow"
        task1.upstream_task_ids = []
        task1._is_mapped = False
        task1.flow_update = "update"
        task1._BaseOperator__init_kwargs = {"flow_update": "update"}
        del task1._task_module

        # Test SageMaker operator
        task2 = Mock()
        task2.task_id = "sagemaker_task"
        task2.task_type = "SageMakerStartPipelineOperator"
        task2.__module__ = "airflow.providers.amazon.aws.operators.sagemaker"
        task2.upstream_task_ids = []
        task2._is_mapped = False
        task2.config = "config"
        task2._BaseOperator__init_kwargs = {"config": "config"}
        del task2._task_module

        with patch("builtins.dir", return_value=["task_id", "task_type", "flow_update", "config"]):
            self.mock_dag_object.tasks = [task1, task2]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        # flow_update should be removed from appflow task
        self.assertNotIn("flow_update", result["appflow_task"])
        # config should be removed from sagemaker task
        self.assertNotIn("config", result["sagemaker_task"])

    def test_skip_dtm_keywords(self):
        """Test skipping DTM keywords"""
        self.mock_validator.validate_field.return_value = True

        task = Mock()
        task.task_id = "test_task"
        task.task_type = "BashOperator"
        task.__module__ = "airflow.operators.bash"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.op_kwargs_expand_input = "expand"
        task.expand_input = "expand"
        task.partial_kwargs = "partial"
        task.param1 = "value1"
        task._BaseOperator__init_kwargs = {"param1": "value1"}

        with patch(
            "builtins.dir",
            return_value=["task_id", "task_type", "op_kwargs_expand_input", "expand_input", "partial_kwargs", "param1"],
        ):
            self.mock_dag_object.tasks = [task]

            result = convert_tasks(self.mock_taskflow_parser, self.mock_dag_object, "test.py", self.mock_validator)

        # DTM keywords should not be in result
        self.assertNotIn("op_kwargs_expand_input", result["test_task"])
        self.assertNotIn("expand_input", result["test_task"])
        self.assertNotIn("partial_kwargs", result["test_task"])
        # Regular param should be included
        self.assertIn("param1", result["test_task"])


class TestOperatorParameters(unittest.TestCase):
    """Test cases for operator constructor parameters and task params"""

    def test_s3_operator_parameters(self):
        """Test that get_constructor_parameter_names returns expected parameters for S3Operator"""
        # Test with S3CreateBucketOperator as an example S3 operator
        parameters = get_constructor_parameter_names(S3CreateBucketOperator)

        # Verify that common S3 operator parameters are included
        expected_params = ["bucket_name", "aws_conn_id", "region_name"]
        for param in expected_params:
            self.assertIn(param, parameters, f"Parameter '{param}' should be in S3Operator parameters")

        # Verify that basic operator parameters are also included
        basic_params = ["task_id", "retries", "retry_delay"]
        for param in basic_params:
            self.assertIn(param, parameters, f"Basic parameter '{param}' should be in operator parameters")

    def test_empty_operator_parameters(self):
        """Test that get_constructor_parameter_names returns expected parameters for EmptyOperator"""
        parameters = get_constructor_parameter_names(EmptyOperator)

        # Verify that basic operator parameters are included
        expected_params = ["task_id", "retries", "retry_delay", "depends_on_past", "wait_for_downstream"]
        for param in expected_params:
            self.assertIn(param, parameters, f"Parameter '{param}' should be in EmptyOperator parameters")

        # Verify that the list is not empty
        self.assertGreater(len(parameters), 0, "EmptyOperator should have at least some parameters")

    def test_params_filtered_when_matching_dag_params(self):
        """Test that task params are filtered when they match DAG params"""
        mock_validator = Mock()
        mock_validator.validate_field.return_value = True
        mock_taskflow_parser = Mock()
        
        # Create DAG with params
        mock_dag = Mock()
        mock_dag.params = {"s3_bucket": "test-bucket", "s3_prefix": "test-prefix"}
        
        # Create task with same params (inherited)
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "S3CreateObjectOperator"
        task.__module__ = "airflow.providers.amazon.aws.operators.s3"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.params = {"s3_bucket": "test-bucket", "s3_prefix": "test-prefix"}
        task._BaseOperator__init_kwargs = {}
        del task._task_module
        
        with patch("builtins.dir", return_value=["task_id", "params"]):
            mock_dag.tasks = [task]
            result = convert_tasks(mock_taskflow_parser, mock_dag, "test.py", mock_validator)
        
        # params should be filtered out since they match DAG params
        self.assertNotIn("params", result["test_task"])

    def test_params_kept_when_different_from_dag_params(self):
        """Test that task params are kept when they differ from DAG params"""
        mock_validator = Mock()
        mock_validator.validate_field.return_value = True
        mock_taskflow_parser = Mock()
        
        # Create DAG with params
        mock_dag = Mock()
        mock_dag.params = {"s3_bucket": "test-bucket"}
        
        # Create task with different params
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "S3CreateObjectOperator"
        task.__module__ = "airflow.providers.amazon.aws.operators.s3"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.params = {"s3_bucket": "different-bucket", "custom_param": "value"}
        task._BaseOperator__init_kwargs = {"params": {"s3_bucket": "different-bucket", "custom_param": "value"}}
        del task._task_module
        
        with patch("builtins.dir", return_value=["task_id", "params"]):
            mock_dag.tasks = [task]
            result = convert_tasks(mock_taskflow_parser, mock_dag, "test.py", mock_validator)
        
        # params should be kept since they differ from DAG params
        self.assertIn("params", result["test_task"])
        self.assertEqual(result["test_task"]["params"], {"s3_bucket": "different-bucket", "custom_param": "value"})

    def test_empty_params_filtered_when_dag_params_empty(self):
        """Test that empty task params are filtered when DAG params are also empty"""
        mock_validator = Mock()
        mock_validator.validate_field.return_value = True
        mock_taskflow_parser = Mock()
        
        # Create DAG with empty params
        mock_dag = Mock()
        mock_dag.params = {}
        
        # Create task with empty params
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "S3CreateObjectOperator"
        task.__module__ = "airflow.providers.amazon.aws.operators.s3"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.params = {}
        task._BaseOperator__init_kwargs = {}
        del task._task_module
        
        with patch("builtins.dir", return_value=["task_id", "params"]):
            mock_dag.tasks = [task]
            result = convert_tasks(mock_taskflow_parser, mock_dag, "test.py", mock_validator)
        
        # empty params should be filtered out
        self.assertNotIn("params", result["test_task"])

    def test_identical_params_filtered_even_when_in_init_kwargs(self):
        """Test that task params are filtered when they match DAG params even if in init_kwargs"""
        mock_validator = Mock()
        mock_validator.validate_field.return_value = True
        mock_taskflow_parser = Mock()
        
        # Create DAG with params
        mock_dag = Mock()
        mock_dag.params = {"s3_bucket": "test-bucket", "s3_prefix": "test-prefix"}
        
        # Create task with identical params in init_kwargs (Airflow auto-adds them)
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "S3CreateObjectOperator"
        task.__module__ = "airflow.providers.amazon.aws.operators.s3"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.params = {"s3_bucket": "test-bucket", "s3_prefix": "test-prefix"}
        task._BaseOperator__init_kwargs = {"params": {"s3_bucket": "test-bucket", "s3_prefix": "test-prefix"}}
        del task._task_module
        
        with patch("builtins.dir", return_value=["task_id", "params"]):
            mock_dag.tasks = [task]
            result = convert_tasks(mock_taskflow_parser, mock_dag, "test.py", mock_validator)
        
        # params should be filtered since they match DAG params (not explicitly different)
        self.assertNotIn("params", result["test_task"])

    def test_params_kept_when_same_key_different_value(self):
        """Test that task params are kept when they have same key but different value from DAG params"""
        mock_validator = Mock()
        mock_validator.validate_field.return_value = True
        mock_taskflow_parser = Mock()
        
        # Create DAG with params
        mock_dag = Mock()
        mock_dag.params = {"s3_bucket": "dag-bucket"}
        
        # Create task with same key but different value
        task = Mock()
        task.task_id = "test_task"
        task.task_type = "S3CreateObjectOperator"
        task.__module__ = "airflow.providers.amazon.aws.operators.s3"
        task.upstream_task_ids = []
        task._is_mapped = False
        task.params = {"s3_bucket": "task-bucket"}
        task._BaseOperator__init_kwargs = {"params": {"s3_bucket": "task-bucket"}}
        del task._task_module
        
        with patch("builtins.dir", return_value=["task_id", "params"]):
            mock_dag.tasks = [task]
            result = convert_tasks(mock_taskflow_parser, mock_dag, "test.py", mock_validator)
        
        # params should be kept since value differs (task overrides DAG)
        self.assertIn("params", result["test_task"])
        self.assertEqual(result["test_task"]["params"], {"s3_bucket": "task-bucket"})
