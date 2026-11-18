import sys
from pathlib import Path
from typing import Annotated

import typer

from dag_converter.code_package import PackageOptions

app = typer.Typer()
app.command()


@app.callback()
def callback():
    """
    CLI tool for converting Python airflow Dags to YAML Dags for DagFactory
    """


@app.command()
def convert(
    input_path: Annotated[Path, typer.Argument(help="Path containing Python Dag(s) to convert")],
    output: Annotated[Path, typer.Option(help="Path to output converted YAML Dag(s) to")] = Path("output_yaml/"),
    bucket: Annotated[str, typer.Option(help="S3 bucket to upload converted Dags to. Uses local AWS credentials")] = "",
    validate: Annotated[bool, typer.Option(help="Validate the output YAML using DagFactory")] = True,
    debug: Annotated[
        bool, typer.Option(help="Enable logging DagBag objects to .initial_task_attributes before and after conversion")
    ] = False,  # noqa 501
    dag_folder: Annotated[
        Path | None, typer.Option(help="Dag folder root, as on the service [default: nearest dags/ folder, see README]")
    ] = None,
    airflow_home: Annotated[
        Path | None, typer.Option(help="AIRFLOW_HOME for parsing [default: $AIRFLOW_HOME, else parent of dags/]")
    ] = None,
    conn: Annotated[
        list[str] | None, typer.Option(help="Connection for parsing as ID=URI (repeatable); others get a placeholder")
    ] = None,
    var: Annotated[list[str] | None, typer.Option(help="Airflow Variable for parsing as KEY=VALUE, repeatable")] = None,
    requirements: Annotated[
        Path | None, typer.Option(help="requirements.txt of the Dags; parse inside a cached virtualenv with them")
    ] = None,
    package: Annotated[bool, typer.Option(help="Also build a <output>/<dag_id>.zip code package per Dag")] = False,
    include: Annotated[
        list[str] | None, typer.Option(help="With --package: glob relative to the Dag folder to add (repeatable)")
    ] = None,
    exclude: Annotated[
        list[str] | None, typer.Option(help="With --package: glob relative to the Dag folder to omit (repeatable)")
    ] = None,
    prefix: Annotated[str, typer.Option(help="With --package and --bucket: S3 key prefix for uploads")] = "",
):
    """
    Loads Python Dags from input and converts them to YAML Dags.
    """
    from dag_converter import __version__
    from dag_converter.console_config import print_info

    print_info(f"dag-converter {__version__} using Python {sys.executable}")

    if requirements:
        from dag_converter.requirements_env import run_in_requirements_env

        raise typer.Exit(run_in_requirements_env(requirements, sys.argv[1:]))

    from dag_converter.parse_env import parse_environment

    with parse_environment(
        input_path, airflow_home=airflow_home, conns=conn or [], variables=var or [], dag_folder=dag_folder
    ):
        # Imported here: Airflow reads its configuration on import, so the parse environment must be set first
        from dag_converter.conversion_manager import ConversionManager

        package_options = PackageOptions(includes=include or [], excludes=exclude or [], prefix=prefix)
        conversion_manager = ConversionManager(s3_bucket=bucket, package_options=package_options if package else None)
        conversion_manager.start_conversion_process(
            dag_file_path=input_path, output_dir=output, user_validate=validate, debug=debug
        )


if __name__ == "__main__":
    app()
