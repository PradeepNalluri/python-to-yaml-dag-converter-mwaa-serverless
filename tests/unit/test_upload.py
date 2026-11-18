"""Tests for upload module (mocked S3 client only)."""

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
            command = upload.upload_package(s3, "bucket", "team/", "my_dag", self.yaml_path, self.zip_path)
        keys = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        self.assertEqual(keys, ["team/dags/my_dag.yaml", "team/code/my_dag.zip"])
        self.assertIn("aws mwaa-serverless create-workflow", command)
        self.assertIn('"ObjectKey": "team/dags/my_dag.yaml", "VersionId": "v-yaml"', command)
        self.assertIn(
            '{"S3Location": {"Bucket": "bucket", "ObjectKey": "team/code/my_dag.zip", "VersionId": "v-zip"}}', command
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
            command = upload.upload_package(s3, "bucket", "", "my_dag", self.yaml_path, self.zip_path)
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
            command = upload.upload_package(s3, "bucket", "team", "my_dag", self.yaml_path, None)
        self.assertEqual([call.kwargs["Key"] for call in s3.put_object.call_args_list], ["team/dags/my_dag.yaml"])
        self.assertIn('"ObjectKey": "team/dags/my_dag.yaml", "VersionId": "v-yaml"', command)
        self.assertNotIn("--code", command)
        self.assertIn("--role-arn <EXECUTION_ROLE_ARN>", command)
