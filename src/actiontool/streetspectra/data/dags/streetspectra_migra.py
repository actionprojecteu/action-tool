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

from airflow.models import Variable

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.operators.email  import EmailOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator


#-----------------------
# custom Airflow imports
# ----------------------

from airflow_actionproject.operators.epicollect5   import EC5ExportEntriesOperator
from airflow_actionproject.operators.zooniverse    import ZooniverseExportOperator, ZooniverseDeltaOperator, ZooniverseTransformOperator
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.action        import ActionDownloadFromVariableDateOperator, ActionUploadOperator, ActionDownloadFromStartDateOperator
from airflow_actionproject.operators.streetspectra import EC5TransformOperator,  SQLInsertObservationsOperator, ZooImportOperator
from airflow_actionproject.operators.streetspectra import PreprocessClassifOperator, AggregateOperator, AggregateCSVExportOperator, IndividualCSVExportOperator
from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets
from airflow_actionproject.callables.action        import check_number_of_entries
from airflow_actionproject.callables.streetspectra import check_new_subjects, check_new_csv_version

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


# ==================================
# Export from the beginning workflow
# ==================================

migra1_start_date = datetime(year=2018, month=1, day=1).strftime("%Y-%m-%d")

migra1_streetspectra_dag = DAG(
    'migra1_streetspectra_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Export all images',
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

migra1_load_sql_ec5_observations = SQLInsertObservationsOperator(
    task_id    = "migra1_load_sql_ec5_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/ec5/street-spectra/migra1-{{ds}}.json",
    dag        = migra1_streetspectra_dag,
)

migra1_download_from_action = ActionDownloadFromStartDateOperator(
    task_id        = "migra1_download_from_action",
    conn_id        = "streetspectra-action-database",
    start_date     = "2018-01-01",
    output_path    = "/tmp/ec5/street-spectra/migra1-action-{{ds}}.json",
    n_entries      = 20000,                                  
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = migra1_streetspectra_dag,
)

migra1_load2_sql_ec5_observations = SQLInsertObservationsOperator(
    task_id    = "migra1_load2_sql_ec5_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/ec5/street-spectra/migra1-action-{{ds}}.json",
    dag        = migra1_streetspectra_dag,
)

migra1_export_ec5_observations   >> migra1_transform_ec5_observations >> migra1_load_sql_ec5_observations
migra1_load_sql_ec5_observations >> migra1_download_from_action       >> migra1_load2_sql_ec5_observations