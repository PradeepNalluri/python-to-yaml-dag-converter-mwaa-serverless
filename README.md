## Python to Yaml Dag Converter for MWAA Serverless

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Python to Yaml Dag Converter for MWAA Serverless is a command-line tool for converting [Apache Airflow®](https://airflow.apache.org) Dags written in Python to [dag-factory](https://github.com/astronomer/dag-factory) compatible Dags written in YAML.

## Prerequisites

- Python 3.11+

## Benefits
- Onboard existing Python Dags to declarative YAML
- Ensures compatibility with DagFactory 1.0
- Convert Python Dags to MWAA Serverless compatible YAML
- Upload converted YAML to S3

## ⚡ Quick Start

### **Step 1**: Install
```
pip install python-to-yaml-dag-converter-mwaa-serverless
```

### **Step 2**: Use
```
dag-converter convert <python-dag-file>
```
Call with the `--help` flag for more options

### **Step 3**: Review
Review the converted Dags in the `output_yaml` folder in the current directory

## Options
* `--help` Show help message and exit
* `--output`: PATH Path to output converted YAML Dag(s) to [default: output_yaml]
* `--bucket`: TEXT S3 bucket to upload converted Dags to. Uses local AWS credentials
* `--validate/--no-validate`: Validate the output YAML using DagFactory
* `--debug/--no-debug` Enable logging DagBag objects to .initial_task_attributes before and after conversion
* `--dag-folder`: PATH Dag folder root (the folder that becomes `/usr/local/airflow/dags` on the service). Defaults to the nearest folder named `dags` among the input's folder (the parent folder for a file) and its two parent folders, otherwise the input's folder
* `--airflow-home`: PATH `AIRFLOW_HOME` used while parsing. Defaults to `$AIRFLOW_HOME` if set, otherwise the folder containing `dags/` (or the input's folder when the input is not inside a `dags` folder)
* `--conn`: ID=URI Connection used while parsing (repeatable), e.g. `--conn 'my_db=postgres://user:pass@host:5432/db'`
* `--var`: KEY=VALUE Airflow Variable used while parsing (repeatable)
* `--requirements`: PATH requirements.txt of the Dags; parse inside a cached virtualenv that contains them (see below)
* `--package/--no-package` Also build a code package `<output>/<dag_id>.zip` per Dag (see below)
* `--include`: PATTERN / `--exclude`: PATTERN With `--package`: glob, relative to the Dag folder, of files to add / omit (repeatable)
* `--prefix`: TEXT With `--package` and `--bucket`: S3 key prefix for uploads

## How Dags are parsed
The converter imports your Dag files with Airflow 3.0.6, so top-level Dag code runs locally. To make that work without a full Airflow setup:
* If no metadata database is configured (`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` or `airflow.cfg`), a throwaway SQLite database is created in a temporary directory and deleted afterwards. Nothing is written to `AIRFLOW_HOME`.
* Connections that are not defined anywhere (`--conn`, your secrets backend, `AIRFLOW_CONN_*` or the metadata database) resolve to a placeholder Connection, with a warning. `--conn` takes precedence over everything else.
* Variables are not faked: a Variable read at parse time must exist (in your secrets backend, `AIRFLOW_VAR_*` or the metadata database) or be passed with `--var`. A secrets backend configured with `AIRFLOW__SECRETS__BACKEND` or in `airflow.cfg` is still consulted.
* The Dag folder root is added to the import path, as on MWAA Serverless.
* Cosmos uses a temporary cache directory (unless `[cosmos] cache_dir` is configured), and no `__pycache__` folders are written into your project.
* The interpreter and converter version are printed at start. If a module is missing, the error names the pip package to install (e.g. `cosmos` is provided by `astronomer-cosmos`).

### `--requirements`
Creates (or reuses) a virtualenv under `~/.cache/dag-converter/envs/<hash>` (keyed by the requirements contents, converter version and Python version), installs the converter and then your requirements for the local platform, and re-runs the same command inside it. The cache key also covers nested `-r`/`-c` files and, when the converter runs from a source checkout, its sources. Dependencies of the converter stay at the versions installed with it; a requirement that excludes one of those versions fails the build with both versions named. `uv` is used when available, otherwise `python -m venv` and pip. Requirements that conflict with packages pre-installed on MWAA Serverless (apache-airflow, apache-airflow-providers-amazon, apache-airflow-providers-duckdb, duckdb, boto3, botocore, dag-factory, pyyaml, lz4) are skipped with a warning, because the service always uses its pre-installed version.

### `--package`
Writes `<output>/<dag_id>.zip` next to `<output>/<dag_id>.yaml` for the `--code` option of `create-workflow`. The zip is extracted to `/usr/local/airflow/dags` on the service, so files keep their path relative to the Dag folder. It contains:
* local modules imported by the Dag (found both from what was imported while parsing and from the `import` statements, recursively), plus their parent `__init__.py` files
* local modules referenced by `python_callable`/`operator` in the YAML, and files or folders under the Dag folder referenced by YAML values (scripts, SQL files, `template_searchpath`, Cosmos `project_dir`, ...)
* third-party packages that are not pre-installed on the service, with their dependencies for linux x86_64 / Python 3.12, at the versions used for parsing (a warning is printed when that differs from the Airflow 3.0.6 constraints), installed with `pip install --no-deps --platform manylinux2014_x86_64 --platform manylinux_2_28_x86_64 --python-version 3.12 --only-binary=:all:`
* anything matched by `--include`

It never contains the Dag files being converted (unless imported by an included module), the output folder, virtualenvs, `.git`, `__pycache__`, `*.pyc` or `.DS_Store`. Likely secrets (`profiles.yml`, `.env`, `*.pem`, `*.key`) are skipped with a warning unless matched by `--include`. `.airflowignore` is not used. Symlinks inside the Dag folder keep their own path in the zip. Absolute paths inside the Dag folder, also as words of a longer string such as a `bash_command`, are rewritten to `/usr/local/airflow/dags/...` in the YAML (the Dag folder itself becomes `/usr/local/airflow/dags`, but is not packaged as a whole: add files used from it with `--include`). `python_callable`/`operator` references to modules next to a Dag in a subfolder are rewritten to their dotted path from the Dag folder (e.g. `sub.my_dag.func`). Other machine-specific paths are reported as warnings. A manifest is printed, packages over 250 MB uncompressed are rejected, and validation runs against the extracted zip. With `--bucket`, the YAML and zip are uploaded to `dags/<dag_id>.yaml` and `code/<dag_id>.zip` (under `--prefix`, if given) and a ready-to-run `aws mwaa-serverless create-workflow` command is printed.

The converter also warns about things MWAA Serverless does not support: reserved or invalid environment variable names read by the Dag, connection ids that cannot be expressed as `AIRFLOW_CONN_*` variables, and deferrable tasks.

## Limitations
* Only AWS operators and AWS provider packages are supported
* Behavior that is not supported in `dag-factory` will not be converted
* Dynamic task mapping conversion is not supported
* Dag files are executed locally at conversion time; the YAML reflects that run (e.g. tasks generated by Cosmos from a dbt project are frozen at conversion time and must be re-converted when the project changes)
* Modules imported dynamically (`importlib.import_module`/`__import__` with non-literal names) cannot be discovered for `--package`; use `--include`

## Examples
* See supported [examples](https://github.com/awslabs/python-to-yaml-dag-converter-mwaa-serverless/blob/main/dev/integ_test/python_test_folder/supported)
* Try one out
```
dag-converter convert airflow_s3.py
```

## Contributing and Bug Reporting

See [CONTRIBUTING](CONTRIBUTING.md#contributing-guidelines) for more information.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
