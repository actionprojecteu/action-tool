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

import requests 
import tabulate

#--------------
# local imports
# -------------

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("streetoool")

# ----------------
# Module constants
# ----------------

# -------------------
# Auxiliary functions
# -------------------

def paging(iterable, headers, size=None, page=10):
    '''
    Pages query output from database and displays in tabular format
    '''
    db_iterable = hasattr(iterable, 'fetchmany')
    while True:
        if db_iterable:
            result = iterable.fetchmany(page)
        else:
            result = list(itertools.islice(iterable, page))
        if len(result) == 0:
            break
        if size is not None:
            size -= page
            if size < 0:
                result = result[:size]  # trim the last rows up to size requested
                print(tabulate.tabulate(result, headers=headers, tablefmt='grid'))
                break
            elif size == 0:
                break
        print(tabulate.tabulate(result, headers=headers, tablefmt='grid'))
        if len(result) < page:
            break
        input("Press Enter to continue [Ctrl-C to abort] ...")
    

def get_image(connection, subject_id):
    filename = os.path.join(os.sep, "tmp", str(subject_id) + '.jpg')
    result = filename
    if os.path.exists(filename):
    	log.info(f"Getting cached image from {filename}")
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
