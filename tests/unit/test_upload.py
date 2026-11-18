"""Tests for upload module (mocked S3 client only)."""

import json
import shlex
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dag_converter import upload


class TestUpload(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.yaml_path = Path(self.temp_dir.name) / "my_dag.yaml"
        self.zip_path = Path(self.temp_dir.name) / "my_dag.zip"
        self.yaml_path.write_text("x")
        self.zip_path.write_bytes(b"zip")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_upload_package_with_versions(self):
        s3 = MagicMock()
        s3.put_object.side_effect = [{"VersionId": "v-yaml"}, {"VersionId": "v-zip"}]
        with (
            patch.object(upload, "print_info"),
            patch.object(upload, "print_progress"),
            patch.object(upload, "print_success"),
        ):
            command = upload.upload_package(s3, "bucket", "my_dag", self.yaml_path, self.zip_path)
        keys = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        self.assertEqual(keys, ["dags/my_dag.yaml", "code/my_dag.zip"])
        self.assertIn("aws mwaa-serverless create-workflow", command)
        self.assertIn('"ObjectKey": "dags/my_dag.yaml", "VersionId": "v-yaml"', command)
        self.assertIn(
            '{"S3Location": {"Bucket": "bucket", "ObjectKey": "code/my_dag.zip", "VersionId": "v-zip"}}', command
        )
        self.assertIn("--role-arn <EXECUTION_ROLE_ARN>", command)

    def test_upload_package_without_versioning(self):
        s3 = MagicMock()
        s3.put_object.return_value = {}
        with (
            patch.object(upload, "print_info"),
            patch.object(upload, "print_progress"),
            patch.object(upload, "print_success"),
        ):
            command = upload.upload_package(s3, "bucket", "my_dag", self.yaml_path, self.zip_path)
        self.assertIn('"ObjectKey": "dags/my_dag.yaml"}', command)
        self.assertNotIn("VersionId", command)

    def test_upload_without_zip(self):
        s3 = MagicMock()
        s3.put_object.return_value = {"VersionId": "v-yaml"}
        with (
            patch.object(upload, "print_info"),
            patch.object(upload, "print_progress"),
            patch.object(upload, "print_success"),
        ):
            command = upload.upload_package(s3, "bucket", "my_dag", self.yaml_path, None)
        self.assertEqual([call.kwargs["Key"] for call in s3.put_object.call_args_list], ["dags/my_dag.yaml"])
        self.assertIn('"ObjectKey": "dags/my_dag.yaml", "VersionId": "v-yaml"', command)
        self.assertNotIn("--code", command)
        self.assertIn("--role-arn <EXECUTION_ROLE_ARN>", command)

    def test_create_workflow_command_environment(self):
        definition = {"Bucket": "b", "ObjectKey": "dags/d.yaml"}
        self.assertNotIn("--environment", upload.create_workflow_command("d", definition, None))
        for region, expected in (("eu-west-1", "eu-west-1"), (None, "<region>")):
            with patch.object(upload.boto3.session.Session, "region_name", region):
                command = upload.create_workflow_command("d", definition, None, upload.secrets_environment())
            args = shlex.split(command.replace("\\\n", " "))
            environment = json.loads(args[args.index("--environment") + 1])
            variables = environment["Variables"]
            self.assertEqual(
                variables["AIRFLOW__WORKERS__SECRETS_BACKEND"],
                "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
            )
            self.assertEqual(
                json.loads(variables["AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS"]),
                {
                    "connections_prefix": "airflow/connections",
                    "variables_prefix": "airflow/variables",
                    "region_name": expected,
                },
            )
        expected_option = (
            """--environment '{"Variables": {"AIRFLOW__WORKERS__SECRETS_BACKEND": """
            """"airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend", """
            """"AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS": "{\\"connections_prefix\\": \\"airflow/connections\\", """
            """\\"variables_prefix\\": \\"airflow/variables\\", \\"region_name\\": \\"<region>\\"}"}}'"""
        )
        self.assertIn(expected_option, command)
