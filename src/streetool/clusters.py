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
import sqlite3
import requests


import numpy as np
import matplotlib.pyplot as plt
import sklearn.cluster as cluster

#--------------
# local imports
# -------------

from actiontool import __version__

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("streetoool")

# ----------------
# Module constants
# ----------------

def get_image(connection, subject_id):
    filename = os.path.join(os.sep, "tmp", str(subject_id) + '.jpg')
    result = filename
    if os.path.exists(filename):
        log.info(f"getting cached image of {subject_id}.jpg") 
    else:
        cursor = connection.cursor()
        cursor.execute('''
            SELECT image_url 
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            ''',
            {'subject_id': subject_id}
        )
        image_url = cursor.fetchone()
        if image_url:
            image_url = image_url[0]
            log.info(f"Downloading image from {image_url}")
            response = requests.get(image_url)
            with open(filename,'wb') as fd:
                fd.write(response.content)
        else:
            result = None 
    return result


def plot(connection, options):
    '''Perform clustering analysis over source light selection'''
    subject_id = int(options.subject_id)
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
    plt.imshow(img, alpha=0,4, zorder=0)
    # create scatter plot for samples from each cluster
    for cl in clusters:
        # get row indexes for samples with this cluster
        row_ix = np.where(yhat == cl)
        plt.scatter(coordinates[row_ix, 0], coordinates[row_ix, 1],  zorder=1)
    plt.show()


def save(connection, options):
    '''Perform clustering analysis over source light selection'''
    log.info("CLUSTERS PLOT")
    cursor = connection.cursor()

    user_selections = cursor.execute("SELECT subject_id, source_x, source_y FROM spectra_classification_t WHERE source_id IS NULL")

    classifications_per_subject = dict()
    for subject_id, source_x, source_y in user_selections:
        coordinates = classifications_per_subject.get(subject_id, [])
        coordinates.append((source_x, source_y))
        classifications_per_subject[subject_id] = coordinates
    
    clustered_classifications = list()
    for subject_id, coordinates in classifications_per_subject.items():
        N_Classifications = len(coordinates)
        if N_Classifications < 2:
            log.debug(f"Skipping cluster analysis in subject {subject_id} [N = {N_Classifications}]")
            clustered_classifications.append({'source_id': 1, 'source_x':coordinates[0][0] , 'source_y':coordinates[0][1]})
        else:
            # Define the model
            coordinates = np.array(coordinates)
            model = cluster.DBSCAN(eps=self.RADIUS, min_samples=2)
            # Fit the model and predict clusters
            yhat = model.fit_predict(coordinates)
            # retrieve unique clusters
            clusters = np.unique(yhat)
            log.info(f"Subject {subject_id}: {len(clusters)} clusters from {N_Classifications} classifications, ids: {clusters}")
                # create scatter plot for samples from each cluster
            for cl in clusters:
                    # get row indexes for samples with this cluster
                    row_ix = np.where(yhat == cl)
                    X = coordinates[row_ix, 0][0]; Y = coordinates[row_ix, 1][0]
                    alist = np.column_stack((X,Y))
                    alist = list( map(lambda t: {'source_id': cl+1 if cl>-1 else cl, 'source_x':t[0], 'source_y': t[1]}, alist))
                    clustered_classifications.extend(alist)
    cursor.executemay(
        '''
        UPDATE spectra_classification_t
        SET source_id = :source_id
        WHERE source_x = :source_x AND source_y = :source_y
        ''',
        clustered_classifications
    )
    connection.commit()