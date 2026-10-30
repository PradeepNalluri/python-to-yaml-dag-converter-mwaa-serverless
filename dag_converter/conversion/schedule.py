from datetime import timedelta
from typing import Any

from airflow.models.dag import DAG
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.trigger import CronTriggerTimetable

from dag_converter.cleanup import get_cleanup_dag, get_timezone_name, typed
from dag_converter.conversion.exceptions import InvalidSchedule


def convert_schedule(dag_object: DAG) -> Any:
    """
    Return the converted schedule.

    Cron strings stay strings, which Airflow turns into a CronTriggerTimetable in the Dag timezone. Timetables that
    differ from that are written as DagFactory '__type__' objects
    """
    schedule = dag_object.schedule

    if schedule is None:
        return None
    if isinstance(schedule, str):
        return schedule
    elif isinstance(schedule, CronTriggerTimetable):
        uses_defaults = (
            not schedule._interval
            and not schedule._run_immediately
            and get_timezone_name(schedule._timezone, None) == get_timezone_name(dag_object.timezone, None)
        )
        if uses_defaults:
            return schedule._expression
        return typed(
            get_timetable_type_path(schedule),
            [schedule._expression],
            timezone=get_timezone_name(schedule._timezone, None),
            interval=get_cleanup_dag(schedule._interval),
            run_immediately=get_cleanup_dag(schedule._run_immediately),
        )
    elif isinstance(schedule, CronDataIntervalTimetable):
        return typed(
            get_timetable_type_path(schedule),
            [schedule._expression],
            timezone=get_timezone_name(schedule._timezone, None),
        )
    elif isinstance(schedule, timedelta):
        raise InvalidSchedule(f"Time delta based schedule is not supported: {schedule}")

    raise InvalidSchedule(f"Schedule {type(schedule).__module__}.{type(schedule).__qualname__} is not supported")


def get_timetable_type_path(timetable: Any) -> str:
    """Public import path of the timetable class, e.g. airflow.timetables.trigger.CronTriggerTimetable"""
    return f"{type(timetable).__module__}.{type(timetable).__qualname__}"
