"""Conversion of operators that are not in the verified operator list of the validator schema."""

import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from dag_converter.cleanup import get_cleanup_dag
from dag_converter.conversion.tasks import convert_tasks
from dag_converter.schema_parser import ArgumentValidator

SCHEMA_FILE = Path(__file__).parent.parent.parent.parent / "dag_converter" / "validator_schema.yaml"


class TestUnverifiedOperatorConversion(unittest.TestCase):
    def setUp(self):
        self.validator = ArgumentValidator(SCHEMA_FILE)

    def _convert(self, dag):
        return get_cleanup_dag(convert_tasks(Mock(), dag, Path("test.py"), self.validator))

    def test_operator_specific_arguments_are_kept(self):
        """Arguments passed by the user are kept even when the schema does not list them"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            SQLExecuteQueryOperator(task_id="run_sql", conn_id="my_db", sql="SELECT 1", autocommit=True)
            KubernetesPodOperator(task_id="pod", image="busybox", cmds=["echo"], namespace="default")

        tasks = self._convert(dag)

        self.assertEqual(
            tasks["run_sql"]["operator"], "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator"
        )
        self.assertEqual(tasks["run_sql"]["conn_id"], "my_db")
        self.assertEqual(tasks["run_sql"]["sql"], "SELECT 1")
        self.assertTrue(tasks["run_sql"]["autocommit"])
        self.assertEqual(tasks["pod"]["image"], "busybox")
        self.assertEqual(tasks["pod"]["cmds"], ["echo"])

    def test_lazy_hook_property_is_not_evaluated(self):
        """Reading SQLExecuteQueryOperator._hook would look up an undefined connection and raise"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            SQLExecuteQueryOperator(task_id="run_sql", conn_id="undefined_connection", sql="SELECT 1")

        tasks = self._convert(dag)

        self.assertNotIn("_hook", tasks["run_sql"])

    def test_structural_arguments_are_dropped(self):
        """The Dag context manager injects 'dag' and 'task_group' into every operator"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            SQLExecuteQueryOperator(task_id="run_sql", conn_id="my_db", sql="SELECT 1")

        tasks = self._convert(dag)

        self.assertNotIn("dag", tasks["run_sql"])
        self.assertNotIn("task_group", tasks["run_sql"])

    def test_unset_sentinel_arguments_are_dropped(self):
        """TriggerDagRunOperator defaults logical_date to the ArgNotSet sentinel"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            TriggerDagRunOperator(task_id="trigger", trigger_dag_id="other_dag")

        tasks = self._convert(dag)

        self.assertNotIn("logical_date", tasks["trigger"])
        self.assertEqual(tasks["trigger"]["trigger_dag_id"], "other_dag")

    def test_string_enum_arguments_use_their_value(self):
        """OnFinishAction is a str Enum and must serialize as 'delete_pod', not 'OnFinishAction.DELETE_POD'"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            KubernetesPodOperator(task_id="pod", image="busybox", on_finish_action="delete_pod")

        tasks = self._convert(dag)

        self.assertEqual(tasks["pod"]["on_finish_action"], "delete_pod")

    def test_inherited_default_args_are_not_repeated_on_tasks(self):
        """default_args not listed in the task schema stay in the Dag default_args unless overridden"""
        default_args = {"owner": "data-team", "email_on_failure": False}
        with DAG(
            dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None, default_args=default_args
        ) as dag:
            SQLExecuteQueryOperator(task_id="inherits", conn_id="my_db", sql="SELECT 1")
            SQLExecuteQueryOperator(task_id="overrides", conn_id="my_db", sql="SELECT 1", owner="other-team")

        tasks = self._convert(dag)

        self.assertNotIn("owner", tasks["inherits"])
        self.assertNotIn("email_on_failure", tasks["inherits"])
        self.assertEqual(tasks["overrides"]["owner"], "other-team")

    def test_schema_value_checks_still_apply(self):
        """Arguments listed in the schema must still pass its value checks"""
        with DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            SQLExecuteQueryOperator(task_id="run_sql", conn_id="my_db", sql="SELECT 1", trigger_rule="all_done")

        self.validator.allowed_fields["task"]["trigger_rule"] = 'enum("all_success", "one_success", required=False)'
        tasks = self._convert(dag)

        self.assertNotIn("trigger_rule", tasks["run_sql"])


if __name__ == "__main__":
    unittest.main()
