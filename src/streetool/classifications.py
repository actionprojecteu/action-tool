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

def dynamic_sql(options):
    columns = list()
    headers = list()
    if options.workflow:
        columns.append("workflow_id")
        headers.append("Workflow Id")
    if options.user:
        columns.append("user_id")
        headers.append("User Id")
    if options.subject:
        columns.append("subject_id")
        headers.append("Subject Id")
    if options.classification:
        columns.append("classification_id")
        headers.append("Classification Id")
    if options.source:
        columns.append("source_id")
        headers.append("Source Id")
    if len(columns) == 0:
        raise ValueError("At least one --<flag> must be specified")
    headers.append("# Classif")
    sql = f"SELECT {','.join(columns)}, COUNT(*) FROM  spectra_classification_v GROUP BY {','.join(columns)} ORDER BY {','.join(columns)}"
    return sql, headers

# ========
# COMMANDS
# ========


def view(connection, options):
    sql, headers = dynamic_sql(options)
    cursor = connection.cursor()
    cursor.execute(sql)
    paging(
        iterable = cursor,
        headers = headers,
    )