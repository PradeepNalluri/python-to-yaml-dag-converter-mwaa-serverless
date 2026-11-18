"""Upload a converted Dag and its code package, if any, to S3 for MWAA Serverless (``--bucket``)."""

import json
import shlex
from pathlib import Path

from dag_converter.console_config import print_error, print_info, print_progress, print_success


def _put(s3_client, bucket: str, key: str, path: Path) -> dict:
    """Upload a file with put_object (which, unlike upload_file, returns the VersionId)."""
    print_progress(f"Uploading {path} to s3://{bucket}/{key}")
    with open(path, "rb") as body:
        response = s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    location = {"Bucket": bucket, "ObjectKey": key}
    if response.get("VersionId"):
        location["VersionId"] = response["VersionId"]
    print_success(
        f"Uploaded s3://{bucket}/{key}" + (f" (version {location['VersionId']})" if "VersionId" in location else "")
    )
    return location


def create_workflow_command(dag_id: str, definition: dict, code: dict | None) -> str:
    return " \\\n  ".join(
        [
            "aws mwaa-serverless create-workflow",
            f"--name {shlex.quote(dag_id)}",
            f"--definition-s3-location {shlex.quote(json.dumps(definition))}",
            *([f"--code {shlex.quote(json.dumps({'S3Location': code}))}"] if code else []),
            "--role-arn <EXECUTION_ROLE_ARN>",
        ]
    )


def upload_package(s3_client, bucket: str, prefix: str, dag_id: str, yaml_path: Path, zip_path: Path | None) -> str:
    """Upload to <prefix>dags/<dag_id>.yaml and, if there is a zip, <prefix>code/<dag_id>.zip; print and return
    the create-workflow command."""
    prefix = f"{prefix.strip('/')}/" if prefix.strip("/") else ""
    try:
        definition = _put(s3_client, bucket, f"{prefix}dags/{dag_id}.yaml", yaml_path)
        code = _put(s3_client, bucket, f"{prefix}code/{dag_id}.zip", zip_path) if zip_path else None
    except Exception as e:
        print_error(f"Failed to upload to S3: {e}")
        raise
    command = create_workflow_command(dag_id, definition, code)
    print_info("Create the workflow with:\n" + command, soft_wrap=True, markup=False)
    return command
