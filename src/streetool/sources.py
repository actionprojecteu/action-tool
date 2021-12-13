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


# ========
# COMMANDS
# ========

def plot(connection, options):
    '''Perform clustering analysis over source light selection'''
    subject_id = options.subject_id
    filename = get_image(connection, subject_id)
    if not filename:
        log.error(f"No image for subject-id {subject_id}")
        return
    img = plt.imread(filename)
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
    model = cluster.DBSCAN(eps=options.radius, min_samples=2)
    # Fit the model and predict clusters
    yhat = model.fit_predict(coordinates)
    # retrieve unique clusters
    clusters = np.unique(yhat)
    log.info(f"Subject {subject_id}: {len(clusters)} clusters from {N_Classifications} classifications, ids: {clusters}")
    plt.imshow(img, alpha=0.5, zorder=0)
    # create scatter plot for samples from each cluster
    for cl in clusters:
        # get row indexes for samples with this cluster
        row_ix = np.where(yhat == cl)
        plt.scatter(coordinates[row_ix, 0], coordinates[row_ix, 1],  marker='+', zorder=1)
    plt.show()

def view(connection, options):
    subject_id = options.subject_id
    cursor = connection.cursor()
    if options.summary:
        cursor.execute('''
            SELECT subject_id, count(*), count(DISTINCT source_id)
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            ''',
            {'subject_id': subject_id}
        )
        header = ("Subject Id", "# Classif.", "# Source Ids")
    elif options.normal:
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