"""Tests for PythonOperator and BashOperator support."""

import os
import tempfile
import unittest
from pathlib import Path

from dag_converter.schema_parser import ArgumentValidator
from dag_converter.static_validator import extract_operator_types, validate_file

SCHEMA_FILE = Path(__file__).parent.parent.parent / "dag_converter" / "validator_schema.yaml"


class OperatorValidationTestCase(unittest.TestCase):
    def setUp(self):
        self.validator = ArgumentValidator(SCHEMA_FILE)

    def _create_file(self, content):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(content)
            return Path(f.name)


class TestPythonOperatorValidation(OperatorValidationTestCase):
    """Test that PythonOperator passes static validation."""

    def test_python_operator_standard_provider_accepted(self):
        """Test that airflow.providers.standard.operators.python.PythonOperator is allowed."""
        allowed = self.validator.get_allowed_operators()
        self.assertTrue(
            any("PythonOperator" in op for op in allowed),
            "PythonOperator should be in allowed operators",
        )

    def test_python_operator_legacy_path_accepted(self):
        """Test that airflow.operators.python.PythonOperator is allowed."""
        allowed = self.validator.get_allowed_operators()
        self.assertIn("airflow.operators.python.PythonOperator", allowed)

    def test_python_operator_new_path_accepted(self):
        """Test that airflow.providers.standard.operators.python.PythonOperator is allowed."""
        allowed = self.validator.get_allowed_operators()
        self.assertIn("airflow.providers.standard.operators.python.PythonOperator", allowed)

    def test_validate_file_with_python_operator(self):
        """Test that a file using PythonOperator passes validation."""
        content = """
from airflow.providers.standard.operators.python import PythonOperator

def my_func():
    pass

task = PythonOperator(task_id="test", python_callable=my_func)
"""
        file_path = self._create_file(content)
        try:
            # Should not raise
            validate_file(file_path, self.validator)
        finally:
            os.unlink(file_path)

    def test_extract_python_operator_type(self):
        """Test that PythonOperator is correctly extracted from a DAG file."""
        content = """
from airflow.providers.standard.operators.python import PythonOperator

task = PythonOperator(task_id="test", python_callable=lambda: None)
"""
        file_path = self._create_file(content)
        try:
            operators = extract_operator_types(file_path)
            self.assertIn("PythonOperator", operators)
        finally:
            os.unlink(file_path)


class TestBashOperatorValidation(OperatorValidationTestCase):
    """Test that BashOperator passes static validation."""

    def test_bash_operator_standard_provider_accepted(self):
        """Test that airflow.providers.standard.operators.bash.BashOperator is allowed."""
        allowed = self.validator.get_allowed_operators()
        self.assertTrue(
            any("BashOperator" in op for op in allowed),
            "BashOperator should be in allowed operators",
        )

    def test_bash_operator_legacy_path_accepted(self):
        """Test that airflow.operators.bash.BashOperator is allowed."""
        allowed = self.validator.get_allowed_operators()
        self.assertIn("airflow.operators.bash.BashOperator", allowed)

    def test_bash_operator_new_path_accepted(self):
        """Test that airflow.providers.standard.operators.bash.BashOperator is allowed."""
        allowed = self.validator.get_allowed_operators()
        self.assertIn("airflow.providers.standard.operators.bash.BashOperator", allowed)

    def test_validate_file_with_bash_operator(self):
        """Test that a file using BashOperator passes validation."""
        content = """
from airflow.providers.standard.operators.bash import BashOperator

task = BashOperator(task_id="test", bash_command="echo hello")
"""
        file_path = self._create_file(content)
        try:
            # Should not raise
            validate_file(file_path, self.validator)
        finally:
            os.unlink(file_path)

    def test_extract_bash_operator_type(self):
        """Test that BashOperator is correctly extracted from a DAG file."""
        content = """
from airflow.operators.bash import BashOperator

task = BashOperator(task_id="test", bash_command="echo hello")
"""
        file_path = self._create_file(content)
        try:
            operators = extract_operator_types(file_path)
            self.assertIn("BashOperator", operators)
        finally:
            os.unlink(file_path)


class TestMixedOperatorValidation(OperatorValidationTestCase):
    """Test files with both PythonOperator and BashOperator."""

    def test_validate_file_with_both_operators(self):
        """Test that a file using both PythonOperator and BashOperator passes validation."""
        content = """
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

def my_func():
    pass

task1 = PythonOperator(task_id="python_task", python_callable=my_func)
task2 = BashOperator(task_id="bash_task", bash_command="echo hello")
"""
        file_path = self._create_file(content)
        try:
            # Should not raise
            validate_file(file_path, self.validator)
        finally:
            os.unlink(file_path)

    def test_validate_file_with_unverified_operator_warns(self):
        """Test that a file with an unverified operator is reported but not rejected."""
        content = """
from airflow.providers.standard.operators.python import PythonOperator

task1 = PythonOperator(task_id="test", python_callable=lambda: None)
task2 = CustomUnverifiedOperator(task_id="custom")
"""
        file_path = self._create_file(content)
        try:
            self.assertEqual(validate_file(file_path, self.validator), ["CustomUnverifiedOperator"])
        finally:
            os.unlink(file_path)


if __name__ == "__main__":
    unittest.main()
