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
from airflow.operators.python import BranchPythonOperator

#-----------------------
# custom Airflow imports
# ----------------------

from actiontool import __version__

from airflow_actionproject import __version__ as __lib_version__
from airflow_actionproject.callables.streetspectra import check_new_csv_version
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.streetspectra.zooniverse import AggregateCSVExportOperator, IndividualCSVExportOperator

# ---------------------
# Default DAG arguments
# ---------------------

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'email'           : ("developer@actionproject.eu",), # CAMBIAR AL VERDADERO EN PRODUCCION
    'email_on_failure': True,                       # CAMBIAR A True EN PRODUCCION
    'email_on_retry'  : False,
    'retries'         : 0,
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

# ==============================
# PUBLICATION TO ZENODO WORKFLOW
# ==============================

streetspectra_publish_dag = DAG(
    'streetspectra_publish_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Zenodo publish workflow',
    #schedule_interval = '@monthly',
    schedule_interval = '30 12 1 * *', # Execute monthly at midday (12:30)
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)


# Export aggregated classifications into CSV file format
# This is valid only for StreetSpectra
export_aggregated_csv = AggregateCSVExportOperator(
    task_id     = "export_aggregated_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/streetspectra/publish/streetspectra-aggregated.csv",
    dag         = streetspectra_publish_dag,
)

# Export individuyal classifications into CSV format
# This is valid only for StreetSpectra
export_individual_csv = IndividualCSVExportOperator(
    task_id     = "export_individual_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/streetspectra/publish/streetspectra-individual.csv",
    dag         = streetspectra_publish_dag,
)

check_new_individual_csv = BranchPythonOperator(
    task_id         = "check_new_individual_csv",
    python_callable = check_new_csv_version,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "input_path"    : "/tmp/streetspectra/publish/streetspectra-individual.csv",
        "input_type"    : "individual",
        "true_task_id"  : "publish_individual_csv",
        "false_task_id" : "join_indiv_publish",
    },
    dag           = streetspectra_publish_dag
)

check_new_aggregated_csv = BranchPythonOperator(
    task_id         = "check_new_aggregated_csv",
    python_callable = check_new_csv_version,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "input_path"    : "/tmp/streetspectra/publish/streetspectra-aggregated.csv",
        "input_type"    : "aggregated",
        "true_task_id"  : "publish_aggregated_csv",
        "false_task_id" : "join_aggr_publish",
    },
    dag           = streetspectra_publish_dag
)


# dataset creators for Zenodo publication
CREATORS = [
    {'name': "Zamorano, Jaime",  'affiliation': "Universidad Complutense de Madrid"},
    {'name': "Gonzalez, Rafael", 'affiliation': "Universidad Complutense de Madrid"},
    {'name': "Garcia, Lucia",    'affiliation': "Universidad Complutense de Madrid"}
]

# Publish the aggregated dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_aggregated_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_aggregated_csv",
    conn_id     = "streetspectra-zenodo",                   # CAMBIAR AL conn_id DE PRODUCCION
    title       = "Street Spectra aggregated classifications",
    file_path   = "/tmp/streetspectra/publish/streetspectra-aggregated.csv",
    description = "CSV file containing aggregated classifications for light sources data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = CREATORS,
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project", 'id': "actionprojecteu"}],
    status      = 'draft',  # either 'draft' or 'published'
    dag         = streetspectra_publish_dag,
)

# Publish the individual dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_individual_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_individual_csv",
    conn_id     = "streetspectra-zenodo",               # CAMBIAR AL conn_id DE PRODUCCION
    title       = "Street Spectra individual classifications",
    file_path   = "/tmp/streetspectra/publish/streetspectra-individual.csv",
    description = "CSV file containing individual classifications for subjects data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = CREATORS,
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project", 'id': "actionprojecteu"}],
    status      = 'draft',  # either 'draft' or 'published'
    dag         = streetspectra_publish_dag,
)


# Clean up temporary files
# clean_up_publish_files = BashOperator(
#     task_id      = "clean_up_classif_files",
#     trigger_rule = "none_failed",    # For execution of just one preceeding branch only
#     bash_command = "rm -f /tmp/publish/*.csv",
#     dag          = streetspectra_aggregate_dag,
# )

join_indiv_publish = DummyOperator(
    task_id      = "join_indiv_publish",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    dag          = streetspectra_publish_dag,
)

join_aggr_publish = DummyOperator(
    task_id      = "join_aggr_publish",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    dag          = streetspectra_publish_dag,
)

clean_up_publish_files = DummyOperator(
    task_id      = "clean_up_publish_files",
    dag          = streetspectra_publish_dag,
)

export_aggregated_csv >> check_new_aggregated_csv >> [publish_aggregated_csv, join_aggr_publish]
export_individual_csv >> check_new_individual_csv >> [publish_individual_csv, join_indiv_publish]
[join_indiv_publish, join_aggr_publish]  >> clean_up_publish_files



if __name__ == '__main__':
    print(f"DAG version: {__version__}, library version {__lib_version__}")