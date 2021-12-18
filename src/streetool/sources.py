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
import statistics

# ---------------------
# Third party libraries
# ---------------------

import PIL

import numpy as np
import matplotlib.pyplot as plt
import sklearn.cluster as cluster
from matplotlib.widgets import Button

#--------------
# local imports
# -------------

from streetool.utils import get_image, paging

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("streetoool")

# ----------------
# Module constants
# ----------------


class Cycler:
    def __init__(self, connection, subject_list, **kwargs):
        self.subject = [] if subject_list is None else subject_list
        self.i = 0
        self.N = len(self.subject)
        self.conn = connection
        self.epsilon = kwargs.get('epsilon',1)
        self.compute = kwargs.get('compute',False)
        self.fix     = kwargs.get('fix',False)
        self.reset()
        if self.compute:
            self.one_compute_step(0)
        else:
            self.one_database_step(0)

    def reset(self):
        self.fig, self.axe = plt.subplots()
        # The dimensions are [left, bottom, width, height]
        # All quantities are in fractions of figure width and height.
        axnext = self.fig.add_axes([0.90, 0.01, 0.095, 0.050])
        self.bnext = Button(axnext, 'Next')
        self.bnext.on_clicked(self.next)
        axprev = self.fig.add_axes([0.79, 0.01, 0.095, 0.050])
        self.bprev = Button(axprev, 'Previous')
        self.bprev.on_clicked(self.prev)
        self.axe.set_xlabel("X, pixels")
        self.axe.set_ylabel("Y, pixels")
        self.axim = None
        self.sca = list()
        self.txt = list()
        self.prev_extent = dict()
      

    def load(self, i):
        subject_id = self.subject[i][0]
        log.info(f"Searching for image whose subject id is {subject_id}")
        filename = get_image(self.conn, subject_id)
        if not filename:
            raise Exception(f"No image for subject-id {subject_id}")
        #img = plt.imread(filename)
        img = PIL.Image.open(filename)
        width, height = img.size
        if self.axim is None:
            self.axim = self.axe.imshow(img, alpha=0.5, zorder=-1, aspect='equal', origin='upper')
            self.prev_extent[(width,height)] = self.axim.get_extent()
        else:
            self.axim.set_data(img)
            ext = self.prev_extent.get((width,height), None)
            # Swap exteent components when current image is rotated respect to the previous
            if ext is None:
                ext = list(self.prev_extent[(height,width)])
                tmp    = ext[1]
                ext[1] = ext[2]
                ext[2] = tmp
                self.prev_extent[(width,height)] = tuple(ext)
            self.axim.set_extent(ext)
            self.axe.relim()
            self.axe.autoscale_view()
        return subject_id


    def update(self, i):
        # remove whats drawn in the scatter plots
        for sca in self.sca:
            sca.remove()
        self.sca = list()
        for txt in self.txt:
            txt.remove()
        self.txt = list()
        if self.compute:
            self.one_compute_step(i)
        else:
            self.one_database_step(i)
        self.fig.canvas.draw_idle()
        self.fig.canvas.flush_events()


    def next(self, event):
        self.i = (self.i +1) % self.N
        self.update(self.i)


    def prev(self, event):
        self.i = (self.i -1 + self.N) % self.N
        self.update(self.i)

        
    def one_database_step(self, i):
        subject_id = self.load(i)
        self.axe.set_title(f'Subject {subject_id}\nLight Sources from the database')
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT DISTINCT cluster_id 
            FROM spectra_classification_v 
            WHERE subject_id = :subject_id
            ORDER BY cluster_id ASC
            ''',
            {'subject_id': subject_id}
        )
        cluster_ids = cursor.fetchall()
        for (cluster_id,) in cluster_ids:
            cursor2 = self.conn.cursor()
            cursor2.execute('''
                SELECT source_x, source_y, epsilon  
                FROM spectra_classification_v 
                WHERE subject_id = :subject_id
                AND cluster_id = :cluster_id
                ''',
                {'subject_id': subject_id, 'cluster_id': cluster_id}
            )        
            coordinates = cursor2.fetchall()
            N_Classifications = len(coordinates)
            log.info(f"Subject {subject_id}: cluster_id {cluster_id} has {N_Classifications} data points")
            X, Y, EPS = tuple(zip(*coordinates))
            Xc = statistics.mean(X); Yc = statistics.mean(Y);
            sca = self.axe.scatter(X, Y,  marker='o', zorder=1)
            self.sca.append(sca)
            txt = self.axe.text(Xc+EPS[0], Yc+EPS[0], cluster_id, fontsize=9, zorder=2)
            self.txt.append(txt)
        
    
    def one_compute_step(self, i):
        fix = self.fix
        epsilon = self.epsilon
        subject_id = self.load(i)
        self.axe.set_title(f'Subject {subject_id}\nDetected light sources by DBSCAN (\u03B5 = {epsilon} px)')
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT source_x, source_y 
            FROM spectra_classification_v 
            WHERE subject_id = :subject_id
            ''',
            {'subject_id': subject_id}
        )
        coordinates = cursor.fetchall()
        N_Classifications = len(coordinates)
        coordinates = np.array(coordinates)
        model = cluster.DBSCAN(eps=epsilon, min_samples=2)
        # Fit the model and predict clusters
        yhat = model.fit_predict(coordinates)
        # retrieve unique clusters
        clusters = np.unique(yhat)
        log.info(f"Subject {subject_id}: {len(clusters)} clusters from {N_Classifications} classifications, ids: {clusters}")
        for cl in clusters:
            # get row indexes for samples with this cluster
            row_ix = np.where(yhat == cl)
            X = coordinates[row_ix, 0][0]; Y = coordinates[row_ix, 1][0]
            if(cl != -1):
                Xc = np.average(X); Yc = np.average(Y)
                sca = self.axe.scatter(X, Y,  marker='o', zorder=1)
                self.sca.append(sca)
                txt = self.axe.text(Xc+epsilon, Yc+epsilon, cl+1, fontsize=9, zorder=2)
                self.txt.append(txt)
            elif fix:
                start = max(clusters)+2 # we will shift also the normal ones ...
                for i in range(len(X)) :
                    cluster_id = start + i
                    sca = self.axe.scatter(X[i], Y[i],  marker='o', zorder=1)
                    self.sca.append(sca)
                    txt = self.axe.text(X[i]+epsilon, Y[i]+epsilon, cluster_id, fontsize=9, zorder=2)
                    self.txt.append(txt)
            else:
                sca = self.axe.scatter(X, Y,  marker='o', zorder=1)
                self.sca.append(sca)
                start = max(clusters)+2 # we will shift also the normal ones ...
                for i in range(len(X)) :
                    txt = self.axe.text(X[i]+epsilon, Y[i]+epsilon, cl, fontsize=9, zorder=2)
                    self.txt.append(txt)



# ========
# COMMANDS
# ========

def purge(connection, options):
    log.info("Purging all source ids from the database")
    cursor = connection.cursor()
    cursor.execute('''
       UPDATE light_sources_t 
       SET cluster_id = NULL , aggregated = NULL 
       WHERE cluster_id IS NOT NULL;
       '''
    )
    connection.commit()

def duplicates(connection, options):
    cursor = connection.cursor()
    cursor.execute('''
       SELECT a.subject_id, a.cluster_id, b.cluster_id, a.source_x, a.source_y
       FROM spectra_classification_v AS a
       JOIN spectra_classification_v AS b 
       ON a.subject_id = b.subject_id AND a.source_x = b.source_x AND a.source_y = b.source_y
       WHERE a.cluster_id < b.cluster_id
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
    compute = options.compute
    if subject_id is not None:
        Cycler(connection, [(subject_id,)], 
            compute = options.compute, 
            epsilon = options.epsilon,
            fix     = options.fix
        )
    else:
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT subject_id FROM spectra_classification_t ORDER BY subject_id")
        Cycler(connection, cursor.fetchall(),
            compute = options.compute, 
            epsilon = options.epsilon,
            fix     = options.fix
        )
    plt.show()

def view(connection, options):
    subject_id = options.subject_id
    cursor = connection.cursor()
    if options.all:
        cursor.execute('''
            SELECT subject_id, count(*), count(DISTINCT cluster_id)
            FROM spectra_classification_t 
            GROUP BY subject_id
            ''',
        )
        header = ("Subject Id", "# Classif.", "# Source Ids")
    elif options.summary:
        if not subject_id:
            raise ValueError("missing --subject-id")
        cursor.execute('''
            SELECT subject_id, count(*), count(DISTINCT cluster_id)
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
            SELECT subject_id, cluster_id, count(*) 
            FROM spectra_classification_t 
            WHERE subject_id = :subject_id
            GROUP BY cluster_id
            ''',
            {'subject_id': subject_id}
        )
        header = ("Subject Id", "Source Id", "# Classif.")
    elif options.detail:
        if not subject_id:
            raise ValueError("missing --subject-id")
        cursor.execute('''
            SELECT subject_id, cluster_id, source_x, source_y, spectrum_type
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