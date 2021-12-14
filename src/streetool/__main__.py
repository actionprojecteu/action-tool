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

import sys
import argparse
import sqlite3
import os.path
import logging
#import logging.handlers
import traceback
import importlib

# -------------
# Local imports
# -------------

from . import  __version__


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("streetoool")

# -----------------------
# Module global functions
# -----------------------

def configureLogging(options):
	if options.verbose:
		level = logging.DEBUG
	elif options.quiet:
		level = logging.WARN
	else:
		level = logging.INFO
	
	log.setLevel(level)
	# Log formatter
	#fmt = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(message)s')
	fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
	# create console handler and set level to debug
	if not options.no_console:
		ch = logging.StreamHandler()
		ch.setFormatter(fmt)
		ch.setLevel(level)
		log.addHandler(ch)
	# Create a file handler
	if options.log_file:
		#fh = logging.handlers.WatchedFileHandler(options.log_file)
		fh = logging.FileHandler(options.log_file)
		fh.setFormatter(fmt)
		fh.setLevel(level)
		log.addHandler(fh)


def python2_warning():
	if sys.version_info[0] < 3:
		log.warning("This software des not run under Python 2 !")


def setup(options):
	python2_warning()
	

def open_database(dbase_path):
	log.info(f"Open/Create database file {dbase_path}")
	os.makedirs(os.path.dirname(dbase_path), exist_ok=True)
	if not os.path.exists(dbase_path):
		with open(dbase_path, 'w') as f:
			pass
		log.info(f"Created database file {dbase_path}")
	return sqlite3.connect(dbase_path)

# =================== #
# THE ARGUMENT PARSER #
# =================== #

def createParser():
	# create the top-level parser
	name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
	parser    = argparse.ArgumentParser(prog=name, description="ACTION NEW PROVISIONING TOOL")

	# Global options
	parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
	parser.add_argument('--dbase', type=str, required=True, help='SQLite database file')
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
	group.add_argument('-q', '--quiet',   action='store_true', help='Quiet output.')
	parser.add_argument('-nk','--no-console', action='store_true', help='Do not log to console.')
	parser.add_argument('--log-file', type=str, default=None, help='Optional log file')

	# --------------------------
	# Create first level parsers
	# --------------------------

	subparser = parser.add_subparsers(dest='command')
	parser_sources = subparser.add_parser('sources', help='sources commands')

	parser_users = subparser.add_parser('users', help='users commands')

	# --------------------------------------
	# Create second level parser for 'users'
	# --------------------------------------

	subparser = parser_users.add_subparsers(dest='subcommand')
	usview = subparser.add_parser('view', help='Display sources summary for a given subject')
	group1 = usview.add_mutually_exclusive_group(required=True)
	group1.add_argument('--all', action='store_true', help='All users')
	group1.add_argument('--anonymous', action='store_true', help='Only anonymous users')
	group1.add_argument('--registered', action='store_true', help='Only anonymous users')
	group2 = usview.add_mutually_exclusive_group(required=True)
	group2.add_argument('--summary', action='store_true', help='How many users have participated.')
	group2.add_argument('--classif', action='store_true', help='Show classifications per user.')
	
	

	# ----------------------------------------
	# Create second level parser for 'sources'
	# ----------------------------------------

	subparser = parser_sources.add_subparsers(dest='subcommand')

	soplot = subparser.add_parser('plot', help='Plot light sources with clustering for a given subject')
	soplot.add_argument('--subject-id',  type=int, required=True, help='Subject id')
	soplot.add_argument('--distance',    type=float, default=30, help='Cluster max distance between points')
	soplot.add_argument('--compute', action='store_true', help='Ciompute clusteres else read from database')

	sopur = subparser.add_parser('purge', help='Purge detected source ids')

	sodup = subparser.add_parser('duplicates', help='Purge detected source ids')

	sosum = subparser.add_parser('view', help='Display sources summary for a given subject')
	sosum.add_argument('--subject-id',  type=int, default=None, help='Subject id')
	group = sosum.add_mutually_exclusive_group(required=True)
	group.add_argument('--all', action='store_true', help='Summary view.')
	group.add_argument('--summary', action='store_true', help='Summary view.')
	group.add_argument('--normal', action='store_true', help='Normal view.')
	group.add_argument('--detail',   action='store_true', help='Detailed view')

	return parser

# ================ #
# MAIN ENTRY POINT #
# ================ #

def main():
	'''
	Utility entry point
	'''
	try:
		options = createParser().parse_args(sys.argv[1:])
		configureLogging(options)
		setup(options)
		connection = open_database(options.dbase)
		name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
		command  = f"{options.command}"
		subcommand = f"{options.subcommand}"
		package="streetool"
		try: 
			command = importlib.import_module(command, package=package)
		except ModuleNotFoundError:	# when debugging module in git source tree ...
			command  = f".{options.command}"
			command = importlib.import_module(command, package=package)
		log.info(f"============== {name} {__version__} ==============")
		getattr(command, subcommand)(connection, options)
	except KeyboardInterrupt as e:
		log.critical("[%s] Interrupted by user ", __name__)
	except Exception as e:
		log.critical("[%s] Fatal error => %s", __name__, str(e) )
		traceback.print_exc()
	finally:
		pass

main()

