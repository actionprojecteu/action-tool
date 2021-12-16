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
        log.info("ENTERING RESET")
        fig, axe = plt.subplots()
        self.fig = fig
        self.axe = axe
        # The dimensions are [left, bottom, width, height]
        # All quantities are in fractions of figure width and height.
        axnext = fig.add_axes([0.90, 0.01, 0.095, 0.050])
        self.bnext = Button(axnext, 'Next')
        self.bnext.on_clicked(self.next)
        axprev = fig.add_axes([0.79, 0.01, 0.095, 0.050])
        self.bprev = Button(axprev, 'Previous')
        self.bprev.on_clicked(self.prev)
        axe.set_xlabel("X, pixels")
        axe.set_ylabel("Y, pixels")
        log.info("END RESET")


    def load(self, i):
        subject_id = self.subject[i][0]
        log.info(f"Searching for image whose subject id is {subject_id}")
        filename = get_image(self.conn, subject_id)
        if not filename:
            raise Exception(f"No image for subject-id {subject_id}")
        img = plt.imread(filename)
        log.info(f"IMG SHAPE {img.shape}")
        self.axe.imshow(img, alpha=0.5, zorder=0, origin='upper')
        return subject_id


    def next(self, event):
        log.info("Next clicked")
        self.i = (self.i +1) % self.N
        #self.reset()
        if self.compute:
            self.one_compute_step(self.i)
        else:
            self.one_database_step(self.i)
        self.fig.canvas.draw_idle()
        self.fig.canvas.flush_events()


    def prev(self, event):
        log.info("Previous clicked")
        self.i = (self.i -1 + self.N) % self.N
        #self.reset()
        if self.compute:
            self.one_compute_step(self.i)
        else:
            self.one_database_step(self.i)
        self.fig.canvas.draw_idle()
        self.fig.canvas.flush_events()

        
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
            self.axe.scatter(X, Y,  marker='o', zorder=1)
            self.axe.text(Xc+EPS[0], Yc+EPS[0], cluster_id, fontsize=9, zorder=2)
        

    
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
                self.axe.scatter(X, Y,  marker='o', zorder=1)
                self.axe.text(Xc+epsilon, Yc+epsilon, cl+1, fontsize=9, zorder=2)
            elif fix:
                start = max(clusters)+2 # we will shift also the normal ones ...
                for i in range(len(X)) :
                    cluster_id = start + i
                    self.axe.scatter(X[i], Y[i],  marker='o', zorder=1)
                    self.axe.text(X[i]+epsilon, Y[i]+epsilon, cluster_id, fontsize=9, zorder=2)
            else:
                self.axe.scatter(X, Y,  marker='o', zorder=1)
                start = max(clusters)+2 # we will shift also the normal ones ...
                for i in range(len(X)) :
                    self.axe.text(X[i]+epsilon, Y[i]+epsilon, cl, fontsize=9, zorder=2)


    



def plot_cluster(connection, subject_id, img, epsilon, fix=False):
    '''Perform clustering analysis over source light selection'''

    cursor = connection.cursor()
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
    plt.title(f'Subject {subject_id}\nDetected light sources by DBSCAN (\u03B5 = {epsilon} px)')
    plt.xlabel("X Coordinates")
    plt.ylabel("Y Coordinates")
    plt.imshow(img, alpha=0.5, zorder=0)
    for cl in clusters:
        # get row indexes for samples with this cluster
        row_ix = np.where(yhat == cl)
        X = coordinates[row_ix, 0][0]; Y = coordinates[row_ix, 1][0]
        
        if(cl != -1):
            Xc = np.average(X); Yc = np.average(Y)
            plt.scatter(X, Y,  marker='o', zorder=1)
            plt.text(Xc+epsilon, Yc+epsilon, cl+1, fontsize=9, zorder=2)
        elif fix:
            start = max(clusters)+2 # we will shift also the normal ones ...
            for i in range(len(X)) :
                cluster_id = start + i
                plt.scatter(X[i], Y[i],  marker='o', zorder=1)
                plt.text(X[i]+epsilon, Y[i]+epsilon, cluster_id, fontsize=9, zorder=2)
        else:
            plt.scatter(X, Y,  marker='o', zorder=1)
            start = max(clusters)+2 # we will shift also the normal ones ...
            for i in range(len(X)) :
                plt.text(X[i]+epsilon, Y[i]+epsilon, cl, fontsize=9, zorder=2)



def plot_dbase(connection, subject_id, img):
    '''Perform clustering analysis over source light selection'''
    cursor = connection.cursor()
    cursor.execute('''
        SELECT DISTINCT cluster_id 
        FROM spectra_classification_v 
        WHERE subject_id = :subject_id
        ORDER BY cluster_id ASC
        ''',
        {'subject_id': subject_id}
    )
    cluster_ids = cursor.fetchall()
    plt.title(f'Subject {subject_id}\nLight Sources from the database')
    plt.xlabel("X Coordinates")
    plt.ylabel("Y Coordinates")
    plt.imshow(img, alpha=0.5, zorder=0)
    for (cluster_id,) in cluster_ids:
        cursor2 = connection.cursor()
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
        plt.scatter(X, Y,  marker='o', zorder=1)
        plt.text(Xc+EPS[0], Yc+EPS[0], cluster_id, fontsize=9, zorder=2)
        

def plot_dbase(connection, subject_id, img, subject_iterable):
    '''Perform clustering analysis over source light selection'''
    cursor = connection.cursor()
    cursor.execute('''
        SELECT DISTINCT cluster_id 
        FROM spectra_classification_v 
        WHERE subject_id = :subject_id
        ORDER BY cluster_id ASC
        ''',
        {'subject_id': subject_id}
    )
    cluster_ids = cursor.fetchall()
    fig, ax = plt.subplots()
    # The dimensions are [left, bottom, width, height]
    # All quantities are in fractions of figure width and height.
    axprev = fig.add_axes([0.79, 0.01, 0.095, 0.050])
    axnext = fig.add_axes([0.90, 0.01, 0.095, 0.050])
    bnext = Button(axnext, 'Next')
    bnext.on_clicked(lambda x: None)
    bprev = Button(axprev, 'Previous')
    bprev.on_clicked(lambda x: None)
    
    ax.set_title(f'Subject {subject_id}\nLight Sources from the database')
    ax.set_xlabel("X Coordinates")
    ax.set_ylabel("Y Coordinates")
    ax.imshow(img, alpha=0.5, zorder=0)
    for (cluster_id,) in cluster_ids:
        cursor2 = connection.cursor()
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
        ax.scatter(X, Y,  marker='o', zorder=1)
        ax.text(Xc+EPS[0], Yc+EPS[0], cluster_id, fontsize=9, zorder=2)
        
 


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


def do_plot(connection, subject_id, options, subject_iterable):
    filename = get_image(connection, subject_id)
    if not filename:
        raise Exception(f"No image for subject-id {subject_id}")
    img = plt.imread(filename)
    if options.compute:
        plot_cluster(connection, subject_id, img, options.distance, options.fix)
    else:
        plot_dbase(connection, subject_id, img, subject_iterable)
    plt.show()


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
    log.info("ENTRAMOS EN EL BUCLE PPAL")
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