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

#-----------------------
# custom Airflow imports
# ----------------------

from actiontool import __version__

from airflow_actionproject import __version__ as __lib_version__
from airflow_actionproject.operators.epicollect5   import EC5ExportEntriesOperator
from airflow_actionproject.operators.action        import ActionUploadOperator
from airflow_actionproject.operators.streetspectra.epicollect5 import EC5TransformOperator
from airflow_actionproject.operators.streetspectra.sqlite import InsertObservationsOperator

# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ("developer@actionproject.eu","astrorafael@gmail.com"), # CAMBIAR AL VERDADERO EN PRODUCCION
    'email_on_failure': True,                       # CAMBIAR A True EN PRODUCCION
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=30),
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

# =========================
# Observations ETL Workflow
# =========================

# 1. Extract from observation sources (currently Epicollect 5)
# 2. Transform into internal format for ACTION PROJECT Database
# 3. Load into ACTION PROJECT Observations Database

streetspectra_collect_dag = DAG(
    'streetspectra_collect_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: collect observations',
    #schedule_interval = '@monthly',
    schedule_interval = '00 06 * * *', # Execute mdayly, early in the morning (06:00)
    start_date        = days_ago(1),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----

export_ec5_observations = EC5ExportEntriesOperator(
    task_id      = "export_ec5_observations",
    conn_id      = "streetspectra-epicollect5",
    start_date   = "{{ds}}",
    end_date     = "{{next_ds}}",
    output_path  = "/tmp/streetspectra/collect/ec5_{{ds}}.json",
    dag          = streetspectra_collect_dag,
)

transform_ec5_observations = EC5TransformOperator(
    task_id      = "transform_ec5_observations",
    input_path   = "/tmp/streetspectra/collect/ec5_{{ds}}.json",
    output_path  = "/tmp/streetspectra/collect/ec5_{{ds}}_transformed.json",
    dag          = streetspectra_collect_dag,
)

load_ec5_observations = ActionUploadOperator(
    task_id    = "load_ec5_observations",
    conn_id    = "streetspectra-action-database",
    input_path = "/tmp/streetspectra/collect/ec5_{{ds}}_transformed.json",
    dag        = streetspectra_collect_dag,
)

# New task to feed SQLite database with Epicollect5 observations
# in pararllel to feeded MongoDB
load2_ec5_observations = InsertObservationsOperator(
    task_id    = "load2_ec5_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/streetspectra/collect/ec5_{{ds}}_transformed.json",
    dag        = streetspectra_collect_dag,
)


clean_up_ec5_files = BashOperator(
    task_id      = "clean_up_ec5_files",
    bash_command = "rm -f /tmp/streetspectra/collect/ec5_{{ds}}*.json",
    dag        = streetspectra_collect_dag,
)

# ---------------------------------------
# Old StreetSpectra Epicollect V project
# included for compatibilty only
# ---------------------------------------

export_ec5_old = EC5ExportEntriesOperator(
    task_id      = "export_ec5_old",
    conn_id      = "oldspectra-epicollect5",
    start_date   = "{{ds}}",
    end_date     = "{{next_ds}}",
    output_path  = "/tmp/streetspectra/collect/ec5_{{ds}}_old.json",
    dag          = streetspectra_collect_dag,
)

transform_ec5_old = EC5TransformOperator(
    task_id      = "transform_ec5_old",
    input_path   = "/tmp/streetspectra/collect/ec5_{{ds}}_old.json",
    output_path  = "/tmp/streetspectra/collect/ec5_{{ds}}_old_transformed.json",
    dag          = streetspectra_collect_dag,
)

load_ec5_old = ActionUploadOperator(
    task_id    = "load_ec5_old",
    conn_id    = "streetspectra-action-database",
    input_path = "/tmp/streetspectra/collect/ec5_{{ds}}_old_transformed.json",
    dag        = streetspectra_collect_dag,
)

# New task to feed SQLite database with Epicollect5 observations
# in pararllel to feeded MongoDB
load2_ec5_old = InsertObservationsOperator(
    task_id    = "load2_ec5_old",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/streetspectra/collect/ec5_{{ds}}_old_transformed.json",
    dag        = streetspectra_collect_dag,
)


# -----------------
# Task dependencies
# -----------------

export_ec5_observations >> transform_ec5_observations >> [load_ec5_observations, load2_ec5_observations] >> clean_up_ec5_files
export_ec5_old          >> transform_ec5_old          >> [load_ec5_old, load2_ec5_old]                   >> clean_up_ec5_files

if __name__ == '__main__':
    print(f"DAG version: {__version__}, library version {__lib_version__}")
