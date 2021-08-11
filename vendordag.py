# -*- coding: utf-8 -*-
""" vendor dag
"""
import os
import datetime
import logging
import psutil

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor

from parser import ParserBase, ReinstatParser


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

DESCRIPTION = """
This DAG is for vendor data process
"""

DAG_NAME = "vendor_data"
OWNER = "cyan"

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

def memory_check(file_name):
    import glob
    mem_size = psutil.virtual_memory().available / 1024 / 1024
    file_size =  os.stat(glob.glob(file_name)[0]).st_size
    if file_size > mem_size:
        logger.info("Could not load whole file in memory. Will load line by line")
        return False
    else:
        return True    

def pre_check_daily_file(**context):
    file_name = f"daily-{context['yesterday_ds']}.csv"
    load_all = memory_check(file_name)
    context["ti"].xcom_push(key="daily_load_all", value=load_all)

def pre_check_reinsta_file(**context):
    file_name = f"historical_reinstatements-*_{context['yesterday_ds']}.csv"
    resinsta_load_all = memory_check(file_name)
    context["ti"].xcom_push(key="reinsta_load_all", value=resinsta_load_all)

def parser(is_daily, **context):
    yesterday_ds = context["yesterday_ds"]
    if is_daily:
        file_name = f"daily-{yesterday_ds}.csv"
        load_all = context["ti"].xcom_pull(task_ids="daily_memory_check", key="daily_load_all")
        parser = ParserBase(is_load_all=load_all, curr_date=yesterday_ds, file_name=file_name)
        parser.delete_curr_data()
    else:
        file_name = f"historical_reinstatements-*_{yesterday_ds}.csv"
        load_all = context["ti"].xcom_pull(task_ids="resinsta_memory_check", key="reinsta_load_all")
        parser = ReinstatParser(is_load_all=load_all, curr_date=yesterday_ds, file_name=file_name)

    parser.run()


with DAG(
    DAG_NAME,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval="@daily",
    start_date=datetime.datetime(2021, 8, 9),
    max_active_runs=1,
    tags=["vendor_data"],
) as dag:

    start = DummyOperator(task_id="all_tasks_start")
    end = DummyOperator(task_id="all_tasks_end")

    daily_file_check_job = FileSensor(
        task_id= "daily_file_sensor_task", 
        poke_interval= 30, 
        filepath="daily-{{yesterday_ds}}.csv"
        )


    reinst_file_check_job = FileSensor(
        task_id= "reinst_file_sensor_task", 
        poke_interval= 30, 
        filepath="historical_reinstatements-*_{{yesterday_ds}}.csv"
        )

    daily_mem_check_job = PythonOperator(
        task_id=f"daily_memory_check",
        python_callable=pre_check_daily_file,
        provide_context=True
    )

    resinsta_mem_check_job = PythonOperator(
        task_id=f"resinsta_memory_check",
        python_callable=pre_check_reinsta_file,
        provide_context=True
    )

    daily_parser_job = PythonOperator(
        task_id=f"daily_parser",
        python_callable=parser,
        op_kwargs={"is_daily": True},
        provide_context=True
    )

    resinsta_parser_job = PythonOperator(
        task_id=f"resinsta_parser",
        python_callable=parser,
        op_kwargs={"is_daily": False},
        provide_context=True
    )

    start >> [daily_file_check_job, reinst_file_check_job]
    daily_file_check_job >> daily_mem_check_job >> daily_parser_job
    reinst_file_check_job >> resinsta_mem_check_job >> resinsta_parser_job
    [daily_parser_job, resinsta_parser_job] >> end

