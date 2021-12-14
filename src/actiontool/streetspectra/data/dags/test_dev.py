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

from airflow_actionproject.operators.action        import ActionRangedDownloadOperator
from airflow_actionproject.operators.streetspectra import ZooImportOperator


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


# ===========================
# Zooniverse Feeding Workflow
# ===========================

streetspectra_feed_test_dag = DAG(
    'streetspectra_feed_test_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Zooniverse image feeding workflow',
    #schedule_interval = '@daily',
    schedule_interval = '10 12 * * *', # Execute daily at midday (12:10)
    start_date        = days_ago(1),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----


download_from_action_test = ActionRangedDownloadOperator(
    task_id        = "download_from_action_test",
    conn_id        = "streetspectra-action-database",
    output_path    = "/tmp/zooniverse/streetspectra/action-{{ds}}.json",
    start_date     = "{{ds}}",
    end_date       = "{{next_ds}}",
    n_entries      = 10,                                    # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = streetspectra_feed_test_dag,
)

upload_new_test_subject_set = ZooImportOperator(
    task_id         = "upload_new_test_subject_set",
    conn_id         = "streetspectra-zooniverse-test",      # CAMBIAR AL conn_id DE PRODUCCION
    input_path      = "/tmp/zooniverse/streetspectra/action-{{ds}}.json", 
    display_name    = "Subject Set {{ds}}",
    dag             = streetspectra_feed_test_dag,
)


# -----------------
# Task dependencies
# -----------------

download_from_action_test >> upload_new_test_subject_set


