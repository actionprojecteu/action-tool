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

import numpy as np
import matplotlib.pyplot as plt
import sklearn.cluster as cluster

#--------------
# local imports
# -------------

from actiontool import __version__

from streetool.utils import get_image, paging

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("streetoool")

# ----------------
# Module constants
# ----------------

def shifted(seq):
        return (seq[1:] + [-1]) if seq[0] == -1 else seq

def plot_cluster(connection, subject_id, img, distance):
    '''Perform clustering analysis over source light selection'''
    cursor = connection.cursor()
    cursor.execute('''
        SELECT source_x, source_y 
        FROM spectra_classification_t 
        WHERE subject_id = :subject_id
        ''',
        {'subject_id': subject_id}
    )
    coordinates = cursor.fetchall()
    N_Classifications = len(coordinates)
    if N_Classifications < 2:
        log.error(f"No cluster for subject {subject_id} [N = {N_Classifications}]")
        return
    coordinates = np.array(coordinates)
    model = cluster.DBSCAN(eps=distance, min_samples=2)
    # Fit the model and predict clusters
    yhat = model.fit_predict(coordinates)
    # retrieve unique clusters
    clusters = np.unique(yhat)
    log.info(f"Subject {subject_id}: {len(clusters)} clusters from {N_Classifications} classifications, ids: {clusters}")
    plt.title('Detected light sources by clustering')
    plt.imshow(img, alpha=0.5, zorder=0)
    # create scatter plot for samples from each cluster
    for cl in shifted(clusters):
        # get row indexes for samples with this cluster
        row_ix = np.where(yhat == cl)
        plt.scatter(coordinates[row_ix, 0], coordinates[row_ix, 1],  marker='o', zorder=1)
    plt.show()


def plot_dbase(connection, subject_id, img):
    '''Perform clustering analysis over source light selection'''
    cursor = connection.cursor()
    cursor.execute('''
        SELECT DISTINCT source_id 
        FROM spectra_classification_t 
        WHERE subject_id = :subject_id
        ORDER BY source_id ASC
        ''',
        {'subject_id': subject_id}
    )
    source_ids = cursor.fetchall()
    log.info(f"SOURCE IDS = {source_ids}")
    plt.title('Light Sources from database')
    plt.imshow(img, alpha=0.5, zorder=0)
    for (source_id,) in source_ids:
        cursor2 = connection.cursor()
        cursor2.execute('''
            SELECT source_x, source_y  
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            AND source_id = :source_id
            ''',
            {'subject_id': subject_id, 'source_id': source_id}
        )        
        coordinates = cursor2.fetchall()
        N_Classifications = len(coordinates)
        log.info(f"Subject {subject_id}: source_id {source_id} has {N_Classifications} data points")
        X, Y = tuple(zip(*coordinates))
        plt.scatter(X, Y,  marker='o', zorder=1)
    plt.show()

# ========
# COMMANDS
# ========

def purge(connection, options):
    log.info("Purging all source ids from the database")
    cursor = connection.cursor()
    cursor.execute('''
       UPDATE spectra_classification_t 
       SET source_id = NULL , aggregated = NULL 
       WHERE source_id IS NOT NULL;
       '''
    )
    connection.commit()

def duplicates(connection, options):
    cursor = connection.cursor()
    cursor.execute('''
       SELECT a.subject_id, a.source_id, b.source_id, a.source_x, a.source_y
       FROM spectra_classification_t AS a
       JOIN spectra_classification_t AS b 
       ON a.subject_id = b.subject_id AND a.source_x = b.source_x AND a.source_y = b.source_y
       WHERE a.source_id < b.source_id
       '''
    )
    headers = ("Subject Id", "Source Id A", "Source Id B", "X", "Y")
    paging(
        iterable = cursor,
        headers  = headers,
    )


def plot(connection, options):
    '''Perform clustering analysis over source light selection'''
    subject_id = options.subject_id
    filename = get_image(connection, subject_id)
    if not filename:
        log.error(f"No image for subject-id {subject_id}")
        return
    img = plt.imread(filename)
    if options.compute:
        plot_cluster(connection, subject_id, img, options.distance)
    else:
        plot_dbase(connection, subject_id, img)

def view(connection, options):
    subject_id = options.subject_id
    cursor = connection.cursor()
    if options.all:
        cursor.execute('''
            SELECT subject_id, count(*), count(DISTINCT source_id)
            FROM spectra_classification_t 
            GROUP BY subject_id
            ''',
        )
        header = ("Subject Id", "# Classif.", "# Source Ids")
    elif options.summary:
        if not subject_id:
            raise ValueError("missing --subject-id")
        cursor.execute('''
            SELECT subject_id, count(*), count(DISTINCT source_id)
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            ''',
            {'subject_id': subject_id}
        )
        header = ("Subject Id", "# Classif.", "# Source Ids")
    elif options.normal:
        if not subject_id:
            raise ValueError("missing --subject-id")
        cursor.execute('''
            SELECT subject_id, source_id, count(*) 
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            GROUP BY source_id
            ''',
            {'subject_id': subject_id}
        )
        header = ("Subject Id", "Source Id", "# Classif.")
    elif options.detail:
        if not subject_id:
            raise ValueError("missing --subject-id")
        cursor.execute('''
            SELECT subject_id, source_id, source_x, source_y, spectrum_type
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            
            ''',
            {'subject_id': subject_id}
        )
        header = ("Subject Id", "Source Id", "X", "Y", "Spectrum")
    paging(
        iterable = cursor,
        headers = header,
    )