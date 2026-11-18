"""Facts about the MWAA Serverless runtime, kept in one place.

Source: https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/custom-code-operators.html
"""

import os
import re
import urllib.request
from pathlib import Path

AIRFLOW_VERSION = "3.0.6"
PYTHON_VERSION = "3.12"
# Wheel platforms of the service (linux x86_64, glibc 2.28+); uv's target also accepts older manylinux tags
PIP_PLATFORMS = ("manylinux2014_x86_64", "manylinux_2_28_x86_64")
UV_PLATFORM = "x86_64-manylinux_2_28"
CONSTRAINTS_URL = (
    f"https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt"
)

# Path the code zip is extracted to (and which is on the import path) on the service
SERVICE_DAGS_DIR = "/usr/local/airflow/dags"
MAX_CODE_SIZE_BYTES = 250 * 1024 * 1024

# Packages pre-installed on the service. These always take precedence over same-named
# packages shipped in the code zip. A value of None means the version is not pinned.
PREINSTALLED_PACKAGES: dict[str, str | None] = {
    "apache-airflow": "3.0.6",
    "apache-airflow-providers-amazon": "9.38.0",
    "apache-airflow-providers-duckdb": "0.2.1",
    "duckdb": "2.0.0",
    "boto3": "1.43.1",
    "botocore": "1.43.1",
    "dag-factory": "1.0.0",
    "pyyaml": None,
    "lz4": "4.4.4",
}


def normalize_name(name: str) -> str:
    """Normalize a distribution name as per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def cache_dir() -> Path:
    """User cache directory for dag-converter (virtualenvs, constraints, staged wheels)."""
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "dag-converter"


def load_constraints() -> dict[str, str]:
    """Return {normalized name: version} from the Airflow constraints file (cached). Empty if unavailable."""
    path = cache_dir() / f"constraints-{AIRFLOW_VERSION}-{PYTHON_VERSION}.txt"
    if not path.exists():
        try:
            with urllib.request.urlopen(CONSTRAINTS_URL, timeout=30) as response:
                text = response.read().decode()
        except Exception:
            return {}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    constraints = {}
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if "==" in line:
            name, version = line.split("==", 1)
            constraints[normalize_name(name)] = version.strip()
    return constraints
