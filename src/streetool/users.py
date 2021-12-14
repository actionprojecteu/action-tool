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


def view(connection, options):
  
    cursor = connection.cursor()
    if options.summary:
        if options.all:
            sql = "SELECT (SELECT count(distinct user_id) FROM spectra_classification_t) + (SELECT count(distinct user_ip) FROM spectra_classification_t WHERE user_id IS NULL)"
        elif options.anonymous:
            sql = "SELECT count(distinct user_ip) FROM spectra_classification_t WHERE user_id IS NULL"
        else:
            sql = "SELECT count(distinct user_id) FROM spectra_classification_t WHERE user_id IS NOT NULL"
        header = ("# Users.",)
        cursor.execute(sql)
    elif options.classif:
        if options.all:
            sql = "SELECT user_id, count(*) FROM spectra_classification_t GROUP BY user_id"
        elif options.anonymous:
            sql = "SELECT user_ip, count(*) FROM spectra_classification_t WHERE user_id IS NULL GROUP BY user_ip"
        else:
            sql = "SELECT user_id, count(*) FROM spectra_classification_t WHERE user_id IS NOT NULL GROUP BY user_id"
        cursor.execute(sql)
        header = ("User Id", "# Classif.")
    else:
        return
    paging(
        iterable = cursor,
        headers = header,
    )