# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------
# Copyright (c) 2021
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

from datetime import datetime, timedelta

# ---------------
# Airflow imports
# ---------------

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

#-----------------------
# custom Airflow imports
# ----------------------

from airflow_actionproject.operators.epicollect5   import EC5ExportEntriesOperator
from airflow_actionproject.operators.action        import ActionDownloadFromStartDateOperator
from airflow_actionproject.operators.streetspectra import EC5TransformOperator,  SQLInsertObservationsOperator

# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ("astrorafael@gmail.com",), # CAMBIAR AL VERDADERO EN PRODUCCION
    'email_on_failure': False,                      # CAMBIAR A True EN PRODUCCION
    'email_on_retry'  : False,
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


# ==================
# Migration workflow
# ==================

# Collects all observations from Epicollect5 and ACTION MongoDB databases
# merging them into a SQLite database

migra1_start_date = datetime(year=2018, month=1, day=1).strftime("%Y-%m-%d")

migra1_streetspectra_dag = DAG(
    'migra1_streetspectra_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Migrate all images to StteetSpectra SQLite',
    #schedule_interval = '@monthly',
    start_date        = days_ago(1),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# This is a cummulative downloading from the beginning
migra1_export_ec5_observations = EC5ExportEntriesOperator(
    task_id      = "migra1_export_ec5_observations",
    conn_id      = "streetspectra-epicollect5",
    start_date   = migra1_start_date,
    end_date     = "{{ds}}",
    output_path  = "/tmp/ec5/street-spectra/migra1-raw-{{ds}}.json",
    dag          = migra1_streetspectra_dag,
)

migra1_transform_ec5_observations = EC5TransformOperator(
    task_id      = "migra1_transform_ec5_observations",
    input_path   = "/tmp/ec5/street-spectra/migra1-raw-{{ds}}.json",
    output_path  = "/tmp/ec5/street-spectra/migra1-{{ds}}.json",
    dag          = migra1_streetspectra_dag,
)

migra1_upload_ec5_observations = SQLInsertObservationsOperator(
    task_id    = "migra1_upload_ec5_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/ec5/street-spectra/migra1-{{ds}}.json",
    dag        = migra1_streetspectra_dag,
)

migra1_download_from_mongo = ActionDownloadFromStartDateOperator(
    task_id        = "migra1_download_from_mongo",
    conn_id        = "streetspectra-action-database",
    start_date     = "2018-01-01",
    output_path    = "/tmp/ec5/street-spectra/migra1-action-{{ds}}.json",
    n_entries      = 20000,                                  
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = migra1_streetspectra_dag,
)

migra1_upload_mongo_observations = SQLInsertObservationsOperator(
    task_id    = "migra1_upload_mongo_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/ec5/street-spectra/migra1-action-{{ds}}.json",
    dag        = migra1_streetspectra_dag,
)

migra1_export_ec5_observations >> migra1_transform_ec5_observations >> migra1_upload_ec5_observations
migra1_upload_ec5_observations >> migra1_download_from_mongo        >> migra1_upload_mongo_observations