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

from actiontool import __version__

from airflow_actionproject import __version__ as __lib_version__
from airflow_actionproject.operators.epicollect5   import EC5ExportEntriesOperator
from airflow_actionproject.operators.zooniverse    import ZooniverseExportOperator, ZooniverseDeltaOperator, ZooniverseTransformOperator
from airflow_actionproject.operators.zenodo        import ZenodoPublishDatasetOperator
from airflow_actionproject.operators.action        import ActionUploadOperator, ActionDownloadFromStartDateOperator

from airflow_actionproject.operators.streetspectra.epicollect5 import EC5TransformOperator
from airflow_actionproject.operators.streetspectra.sqlite import InsertObservationsOperator, ActionDownloadFromVariableDateOperator
from airflow_actionproject.operators.streetspectra.zooniverse import ZooImportOperator
from airflow_actionproject.operators.streetspectra.zooniverse import PreprocessClassifOperator, AggregateOperator, AggregateCSVExportOperator, IndividualCSVExportOperator

from airflow_actionproject.callables.zooniverse    import zooniverse_manage_subject_sets
from airflow_actionproject.callables.streetspectra import check_number_of_entries
from airflow_actionproject.callables.streetspectra import check_new_subjects, check_new_csv_version


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
    schedule_interval = '@daily',
    start_date        = days_ago(1),
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
    remote_path = "Street-Spectra/street-spectra_map.html", # relatice to a doc root setup by ssh_conn_id
    dag        = streetspectra_maps_dag,
)

map_sync_images = ImagesSyncOperator(
    task_id     = "map_sync_images",
    sql_conn_id = "streetspectra-db",
    ssh_conn_id = "streetspectra-guaix",
    temp_dir    = "/tmp/streetspectra/maps/images",
    remote_slug= "Street-Spectra/StreetSpectra_pictures",
    project     = 'street-spectra',
    dag         = streetspectra_maps_dag,
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

map_sync_images >> map_export_observations >> map_add_classifications >> map_sync_metadata
map_sync_metadata >> map_generate_html >> map_copy_html 

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
    output_path  = "/tmp/streetspectra/migra/ec5_raw_{{ds}}.json",
    dag          = migra1_streetspectra_dag,
)

migra1_transform_ec5_observations = EC5TransformOperator(
    task_id      = "migra1_transform_ec5_observations",
    input_path   = "/tmp/streetspectra/migra/ec5_raw_{{ds}}.json",
    output_path  = "/tmp/streetspectra/migra/ec5_{{ds}}.json",
    dag          = migra1_streetspectra_dag,
)

migra1_upload_ec5_observations = InsertObservationsOperator(
    task_id    = "migra1_upload_ec5_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/streetspectra/migra/ec5_{{ds}}.json",
    dag        = migra1_streetspectra_dag,
)

migra1_download_from_mongo = ActionDownloadFromStartDateOperator(
    task_id        = "migra1_download_from_mongo",
    conn_id        = "streetspectra_action-database",
    start_date     = "2018-01-01",
    output_path    = "/tmp/streetspectra/migra/action_{{ds}}.json",
    n_entries      = 20000,                                  
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = migra1_streetspectra_dag,
)

migra1_upload_mongo_observations = InsertObservationsOperator(
    task_id    = "migra1_upload_mongo_observations",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/streetspectra/migra/action_{{ds}}.json",
    dag        = migra1_streetspectra_dag,
)

migra1_export_ec5_observations >> migra1_transform_ec5_observations >> migra1_upload_ec5_observations
migra1_upload_ec5_observations >> migra1_download_from_mongo        >> migra1_upload_mongo_observations


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
    schedule_interval = '0 12 * * *', # Execute daily at midday (12:00)
    start_date        = datetime(year=2019, month=1, day=1),
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

# ===========================
# Zooniverse Feeding Workflow
# ===========================

# Zooniverse Dataset size
# Recommended between 100 and 500 for production environments
N_ENTRIES = 10 # ESTO TIENE QUE CAMBIARSE A 500 PARA PRODUCCION

streetspectra_feed_dag = DAG(
    'streetspectra_feed_dag',
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

manage_subject_sets = ShortCircuitOperator(
    task_id         = "manage_subject_sets",
    python_callable = zooniverse_manage_subject_sets,
    op_kwargs = {
        "conn_id"  : "streetspectra-zooniverse-test",       # CAMBIAR AL conn_id DE PRODUCCION
        "threshold": 75,    # 75% workflow completion status
    },
    dag           = streetspectra_feed_dag
)

check_enough_observations = BranchPythonOperator(
    task_id         = "check_enough_observations",
    python_callable = check_number_of_entries,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "start_date"    : Variable.get("streetspectra_read_tstamp"),  
        "n_entries"     : N_ENTRIES,                              
        "project"       : "street-spectra",
        "true_task_id"  : "download_from_action",
        "false_task_id" : "email_no_images",
        "obs_type"      : 'observation',
    },
    dag           = streetspectra_feed_dag
)

email_no_images = EmailOperator(
    task_id      = "email_no_images",
    to           = ("astrorafael@gmail.com",),
    subject      = "[StreetSpectra] Airflow warn: No ACTION images left",
    html_content = "Not enough images left in ACTION database to create a new Zooniverse Subject Set.",
    dag          = streetspectra_feed_dag,
)

download_from_action = ActionDownloadFromVariableDateOperator(
    task_id        = "download_from_action",
    conn_id        = "streetspectra-db",
    output_path    = "/tmp/streetspectra/feed/action_{{ds}}.json",
    variable_name  = "streetspectra_read_tstamp",
    n_entries      = N_ENTRIES,                                    
    project        = "street-spectra", 
    obs_type       = "observation",
    dag            = streetspectra_feed_dag,
)

upload_new_subject_set = ZooImportOperator(
    task_id         = "upload_new_subject_set",
    conn_id         = "streetspectra-zooniverse-test",      # CAMBIAR AL conn_id DE PRODUCCION
    input_path      = "/tmp/streetspectra/feed/action_{{ds}}.json", 
    display_name    = "Subject Set {{ds}}",
    dag             = streetspectra_feed_dag,
)

# This needs to be configured:
# WARNING - section/key [smtp/smtp_user] not found in config
# See https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email

email_new_subject_set = EmailOperator(
    task_id      = "email_new_subject_set",
    to           = ("astrorafael@gmail.com",),      # Cambiar al email de producción
    subject      = "[StreetSpectra] Airflow info: new Zooniverse Subject Set",
    html_content = "New Zooniverse Subject Set {{ds}} created.",
    dag          = streetspectra_feed_dag,
)

cleanup_action_obs_file = BashOperator(
    task_id      = "cleanup_action_obs_file",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    bash_command = "rm -f /tmp/streetspectra/feed/action_{{ds}}.json",
    dag          = streetspectra_feed_dag,
)

# -----------------
# Task dependencies
# -----------------

manage_subject_sets  >> check_enough_observations >> [download_from_action,  email_no_images]
download_from_action >> upload_new_subject_set >> email_new_subject_set
[email_new_subject_set, email_no_images] >> cleanup_action_obs_file


# ====================================================
# CLASSIFICATIONS AGGREGATION AND PUBLICATION WORKFLOW
# ====================================================

# Aqui hay que tener en cuenta que el exportado de Zooniverse es completo
# y que la BD de ACTION NO detecta duplicados
# Asi que hay que usar ventanas de clasificaciones subida
# Este "enventanado" debe ser lo primero que se haga tras la exportacion para evitar
# que los procesados posteriores sean largos.
# Este enventanado no se hace por variables de Airflow 
# sino en una tabla de la BD Sqlite interna


streetspectra_aggregate_dag = DAG(
    'streetspectra_aggregate_dag',
    default_args      = default_args,
    description       = 'StreetSpectra: Zooniverse classifications export workflow',
    #schedule_interval = '@monthly',
    schedule_interval = '30 12 1 * *', # Execute monthly at midday (12:30)
    start_date        = days_ago(2),
    tags              = ['StreetSpectra', 'ACTION PROJECT'],
)

# -----
# Tasks
# -----

# Perform the whole Zooniverse export from the beginning of the project
export_classifications = ZooniverseExportOperator(
    task_id     = "export_classifications",
    conn_id     = "streetspectra-zooniverse-test",      # CAMBIAR AL conn_id DE PRODUCCION
    output_path = "/tmp/aggregate/zoo_complete_{{ds}}.json",
    generate    = False, 
    wait        = False, 
    timeout     = 600,
    dag         = streetspectra_aggregate_dag,
)


# export_classifications = BashOperator(
#     task_id     = "export_classifications",
#     bash_command = "mkdir -p /tmp/zooniverse; cp /home/rafa/repos/action-tool/complete-*.json /tmp/zooniverse",
#     dag         = streetspectra_aggregate_dag,
# )


# Produces an output file with only new classifications
# made since the last export
# This valid for any Zooniverse project
only_new_classifications = ZooniverseDeltaOperator(
    task_id       = "only_new_classifications",
    conn_id       = "streetspectra-db",
    input_path    = "/tmp/streetspectra/aggregate/zoo_complete_{{ds}}.json",
    output_path   = "/tmp/streetspectra/aggregate/zoo_subset_{{ds}}.json",
    start_date_threshold  = "2021-09-01",
    dag           = streetspectra_aggregate_dag,
)

# Transforms the new Zooniverse classifcations file in to a JSON
# file suitable to be loaded into the ACTION database as 'classifications'
# This is valid for any project that uses the ACTION database API
transform_classifications = ZooniverseTransformOperator(
    task_id      = "transform_classifications",
    input_path   = "/tmp/streetspectra/aggregate/zoo_subset_{{ds}}.json",
    output_path  = "/tmp/streetspectra/aggregate/zoo_transformed-subset_{{ds}}.json",
    project      = "street-spectra",
    dag          = streetspectra_aggregate_dag,
)

# Preprocess the transformed file into a StreetSpectra
# relational database, getting only the most relevant information
# This is valid only for StreetSpectra
preprocess_classifications = PreprocessClassifOperator(
    task_id    = "preprocess_classifications",
    conn_id    = "streetspectra-db",
    input_path = "/tmp/streetspectra/aggregate/zoo_transformed-subset_{{ds}}.json",
    dag        = streetspectra_aggregate_dag,
)

# Check new spectra to be classified and aggregated
# This is valid only for StreetSpectra
check_new_spectra = BranchPythonOperator(
    task_id         = "check_new_spectra",
    python_callable = check_new_subjects,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "true_task_id"  : "aggregate_classifications",
        "false_task_id" : "skip_to_end",
    },
    dag           = streetspectra_aggregate_dag
)

# needed for branching
skip_to_end = DummyOperator(
    task_id    = "skip_to_end",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    dag        = streetspectra_aggregate_dag,
)


# Aggregates classifications into a single combined value 
# for every light source selected by Zooniverse users when classifying
# This is valid only for StreetSpectra
aggregate_classifications = AggregateOperator(
    task_id    = "aggregate_classifications",
    conn_id    = "streetspectra-db",
    distance   = 25,    # max distance to belong to a cluster
    dag        = streetspectra_aggregate_dag,
)

# Export aggregated classifications into CSV file format
# This is valid only for StreetSpectra
export_aggregated_csv = AggregateCSVExportOperator(
    task_id     = "export_aggregated_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/streetspectra/aggregate/streetspectra-aggregated.csv",
    dag         = streetspectra_aggregate_dag,
)

# Export individuyal classifications into CSV format
# This is valid only for StreetSpectra
export_individual_csv = IndividualCSVExportOperator(
    task_id     = "export_individual_csv",
    conn_id     = "streetspectra-db",
    output_path = "/tmp/streetspectra/aggregate/streetspectra-individual.csv",
    dag         = streetspectra_aggregate_dag,
)

check_new_individual_csv = BranchPythonOperator(
    task_id         = "check_new_individual_csv",
    python_callable = check_new_csv_version,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "input_path"    : "/tmp/streetspectra/aggregate/streetspectra-individual.csv",
        "input_type"    : "individual",
        "true_task_id"  : "publish_individual_csv",
        "false_task_id" : "join_indiv_published",
    },
    dag           = streetspectra_aggregate_dag
)

check_new_aggregated_csv = BranchPythonOperator(
    task_id         = "check_new_aggregated_csv",
    python_callable = check_new_csv_version,
    op_kwargs = {
        "conn_id"       : "streetspectra-db",
        "input_path"    : "/tmp/streetspectra/aggregate/streetspectra-aggregated.csv",
        "input_type"    : "aggregated",
        "true_task_id"  : "publish_aggregated_csv",
        "false_task_id" : "join_aggre_published",
    },
    dag           = streetspectra_aggregate_dag
)


# needed for parallel export+publish operations
join_aggre_published = DummyOperator(
    task_id      = "join_aggre_published",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    dag          = streetspectra_aggregate_dag,
)

join_indiv_published = DummyOperator(
    task_id      = "join_indiv_published",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    dag          = streetspectra_aggregate_dag,
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
    conn_id     = "streetspectra-zenodo-sandbox",                   # CAMBIAR AL conn_id DE PRODUCCION
    title       = "Street Spectra aggregated classifications",
    file_path   = "/tmp/streetspectra/aggregate/streetspectra-aggregated.csv",
    description = "CSV file containing aggregated classifications for light sources data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = CREATORS,
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project", 'id': "actionprojecteu"}],
    status      = 'draft',  # either 'draft' or 'published'
    dag         = streetspectra_aggregate_dag,
)

# Publish the individual dataset to Zenodo
# This operator is valid for anybody wishing to publish datasets to Zenodo
publish_individual_csv = ZenodoPublishDatasetOperator(
    task_id     = "publish_individual_csv",
    conn_id     = "streetspectra-zenodo-sandbox",               # CAMBIAR AL conn_id DE PRODUCCION
    title       = "Street Spectra individual classifications",
    file_path   = "/tmp/streetspectra/aggregate/streetspectra-individual.csv",
    description = "CSV file containing individual classifications for subjects data and metadata.",
    version     = '{{ execution_date.strftime("%y.%m")}}',
    creators    = CREATORS,
    communities = [{'title': "Street Spectra", 'id': "street-spectra"}, {'title':"Action Project", 'id': "actionprojecteu"}],
    status      = 'draft',  # either 'draft' or 'published'
    dag         = streetspectra_aggregate_dag,
)


# needed for parallel export+publish operations
join_published = DummyOperator(
    task_id    = "join_published",
    dag        = streetspectra_aggregate_dag,
)

# Clean up temporary files
# clean_up_classif_files = BashOperator(
#     task_id      = "clean_up_classif_files",
#     trigger_rule = "none_failed",    # For execution of just one preceeding branch only
#     bash_command = "rm /tmp/aggregate/*.csv; rm /tmp/aggregate/*_{{ds}}.json",
#     dag          = streetspectra_aggregate_dag,
# )

clean_up_classif_files = DummyOperator(
    task_id      = "clean_up_classif_files",
    trigger_rule = "none_failed",    # For execution of just one preceeding branch only
    dag          = streetspectra_aggregate_dag,
)



# -----------------
# Task dependencies
# -----------------

export_classifications >> only_new_classifications >> transform_classifications >> preprocess_classifications
preprocess_classifications >> check_new_spectra >> [aggregate_classifications, skip_to_end]
aggregate_classifications >> [export_aggregated_csv, export_individual_csv]
export_aggregated_csv >> check_new_aggregated_csv >> [publish_aggregated_csv, join_aggre_published]
export_individual_csv >> check_new_individual_csv >> [publish_individual_csv, join_indiv_published]
[join_aggre_published, join_indiv_published] >> join_published
[join_published, skip_to_end] >> clean_up_classif_files

if __name__ == '__main__':
    print(f"DAG version: {__version__}, library version {__lib_version__}")