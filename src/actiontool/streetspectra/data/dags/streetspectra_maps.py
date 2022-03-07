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
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.operators.email  import EmailOperator

#-----------------------
# custom Airflow imports
# ----------------------

from actiontool import __version__

from airflow_actionproject import __version__ as __lib_version__
from airflow_actionproject.operators.ssh import SCPOperator
from airflow_actionproject.operators.streetspectra.sqlite import ActionRangedDownloadOperator
from airflow_actionproject.operators.streetspectra.maps   import AddClassificationsOperator, FoliumMapOperator
from airflow_actionproject.operators.streetspectra.maps   import ImagesSyncOperator, MetadataSyncOperator

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


map_start_date = datetime(year=2018, month=1, day=1).strftime("%Y-%m-%d")

streetspectra_maps_dag = DAG(
    'streetspectra_maps_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: HTML maps',
    start_date        = days_ago(1),
    schedule_interval = '10 06 * * *', # Execute mdayly, early in the morning (06:10)
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# This is a cummulative downloading from the beginning
map_export_observations = ActionRangedDownloadOperator(
    task_id      = "map_export_observations",
    conn_id      = "streetspectra-db",
    start_date   = map_start_date,
    end_date     = "{{ds}}",
    project      = 'street-spectra',
    output_path  = "/tmp/streetspectra/maps/observations_{{ds}}.json",
    dag          = streetspectra_maps_dag,
)

# This is a cummulative downloading from the beginning
map_add_classifications = AddClassificationsOperator(
    task_id      = "map_add_classifications",
    conn_id      = "streetspectra-db",
    input_path   = "/tmp/streetspectra/maps/observations_{{ds}}.json",
    output_path  = "/tmp/streetspectra/maps/observations_with_classifications_{{ds}}.json",
    dag          = streetspectra_maps_dag,
)

map_generate_html = FoliumMapOperator(
    task_id      = "map_generate_html",
    input_path   = "/tmp/streetspectra/maps/observations_with_classifications_{{ds}}.json",
    output_path  = "/tmp/streetspectra/maps/streetspectra_map.html",
    ssh_conn_id  = "streetspectra-guaix",
    imag_remote_slug = "Street-Spectra/StreetSpectra_pictures",
    meta_remote_slug = "Street-Spectra/StreetSpectra_metadata",
    center_longitude = -3.726111,
    center_latitude  = 40.45111,
    dag          = streetspectra_maps_dag,
)

map_copy_html = SCPOperator(
    task_id     = "map_copy_html",
    ssh_conn_id = "streetspectra-guaix",
    local_path  = "/tmp/streetspectra/maps/streetspectra_map.html",
    remote_path = "Street-Spectra/street-spectra_map.html", # relative to a doc root setup by ssh_conn_id
    dag        = streetspectra_maps_dag,
)

map_sync_images = ImagesSyncOperator(
    task_id    = "map_sync_images",
    sql_conn_id= "streetspectra-db",
    ssh_conn_id= "streetspectra-guaix",
    temp_dir   = "/tmp/streetspectra/maps/images",
    remote_slug= "Street-Spectra/StreetSpectra_pictures",
    project    = 'street-spectra',
    dag        = streetspectra_maps_dag,
)

map_sync_metadata = MetadataSyncOperator(
    task_id    = "map_sync_metadata",
    sql_conn_id= "streetspectra-db",
    ssh_conn_id= "streetspectra-guaix",
    input_path = "/tmp/streetspectra/maps/observations_with_classifications_{{ds}}.json",
    temp_dir   = "/tmp/streetspectra/maps/metadata",
    remote_slug= "Street-Spectra/StreetSpectra_metadata",
    project    = 'street-spectra',
    dag        = streetspectra_maps_dag,
)


map_cleanup_files = BashOperator(
    task_id      = "map_cleanup_files",
    bash_command = "rm -f /tmp/streetspectra/maps/*_{{ds}}.json; rm -f /tmp/streetspectra/maps/streetspectra_map.html",
    dag          = streetspectra_maps_dag,
)

map_sync_images >> map_export_observations >> map_add_classifications >> map_sync_metadata
map_sync_metadata >> map_generate_html >> map_copy_html >> map_cleanup_files


if __name__ == '__main__':
    print(f"DAG version: {__version__}, library version {__lib_version__}")