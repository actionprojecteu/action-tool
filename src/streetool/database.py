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

import os
import os.path
import logging

#--------------
# local imports
# -------------

from streetool.utils import paging

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("streetoool")

# ----------------
# Module constants
# ----------------


def purge_zoo_export_t(connection):
    log.info("Deleting all contents from table zoo_export_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM zoo_export_t')

def purge_export_window_t(connection):
    log.info("Deleting all contents from table zoo_export_window_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM zoo_export_window_t')

def purge_light_sources_t(connection):
    log.info("Deleting all contents from table light_sources_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM light_sources_t')

def purge_spectra_classification_t(connection):
    log.info("Deleting all contents from table spectra_classification_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM spectra_classification_t')

def purge_epicollect5_t(connection):
    log.info("Deleting all contents from table epicollect5_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM epicollect5_t')

def purge_images_t(connection):
    log.info("Deleting all contents from table images_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM images_t')

def purge_metadata_files_t(connection):
    log.info("Deleting all contents from table metadata_files_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM metadata_files_t')

def purge_zenodo_csv_t(connection):
    log.info("Deleting all contents from table zenodo_csv_t")
    cursor = connection.cursor()
    cursor.execute('DELETE FROM zenodo_csv_t')


def purge_classifications(connection):
    purge_light_sources_t(connection)
    purge_spectra_classification_t(connection)
    purge_export_window_t(connection)
    purge_zoo_export_t(connection)
  

def purge_publishing(connection):
    purge_zenodo_csv_t(connection)


def purge_collection(connection):
    purge_epicollect5_t(connection)

def purge_maps(connection):
    purge_images_t(connection)
    purge_metadata_files_t(connection)

# ========
# COMMANDS
# ========


def purge(connection, options):
    if options.all:
        purge_classifications(connection)
        purge_collection(connection)
        purge_publishing(connection)
        connection.commit()
    elif options.classif:
        purge_classifications(connection)
        connection.commit()
    elif options.publ:
        purge_publishing(connection)
        connection.commit()
    elif options.collect:
        purge_collection(connection)
        connection.commit()
    elif options.maps:
        purge_maps(connection)
        connection.commit()
    else:
        raise ValueError("Command line option not recognized") 