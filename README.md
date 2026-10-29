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

## Limitations
* Operators outside the verified list in `validator_schema.yaml` are converted with a warning. Their provider packages must be installed where the converter runs
* Behavior that is not supported in `dag-factory` will not be converted
* Dynamic task mapping with `expand_kwargs` is not supported
* Operators and argument classes must be importable where the YAML Dag runs. Classes defined in the Dag file are rejected
* Callables are supported in `python_callable`, `execution_date_fn` and `on_*_callback` arguments, which `dag-factory` imports. They must be module-level functions: lambdas, nested functions, methods and `functools.partial` values are rejected, and so is more than one callback per callback argument. Callables in other arguments (e.g. `S3KeySensor.check_fn`) are rejected
* Values must be primitives, containers, enums, dataclasses, named tuples, datetime values, `Decimal` or paths. Other objects (including pendulum `Interval`) are rejected, and so are dict keys other than str, int, float and bool
* Attributes assigned after creating an operator are converted when they are arguments of the operator that `__init__` stores as passed. Other assignments are rejected, or ignored with a warning when the operator does not define the attribute
* Schedules: cron strings, `CronTriggerTimetable` and `CronDataIntervalTimetable` are supported
* Dag arguments `dag-factory` does not pass to the Dag are rejected when set: `user_defined_macros`, `user_defined_filters`, `fail_fast`, `fail_stop`, `auto_register`, `disable_bundle_versioning`, `max_consecutive_failed_dag_runs`, `owner_links`, `jinja_environment_kwargs`, `template_undefined`
* PythonOperator, BranchPythonOperator and PythonSensor subclasses that set `python_callable` themselves are rejected, because `dag-factory` requires `python_callable` in the YAML

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
