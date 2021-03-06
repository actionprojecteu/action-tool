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

log = logging.getLogger("actiontool")

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
	

# =================== #
# THE ARGUMENT PARSER #
# =================== #

def createParser():
	# create the top-level parser
	name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
	parser    = argparse.ArgumentParser(prog=name, description="ACTION NEW PROVISIONING TOOL")

	# Global options
	parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
	group.add_argument('-q', '--quiet',   action='store_true', help='Quiet output.')
	parser.add_argument('-nk','--no-console', action='store_true', help='Do not log to console.')
	parser.add_argument('--log-file', type=str, default=None, help='Optional log file')

	
	# --------------------------
	# Create first level parsers
	# --------------------------
	subparser = parser.add_subparsers(dest='pilot')
	parser_ss = subparser.add_parser('streetspectra', help='StreetSpectra pilot')
	
	

	# ---------------------------------------------
	# Create second level parser for StreetSpectra
	# ---------------------------------------------
	subparser = parser_ss.add_subparsers(dest='command')
	parser_dags = subparser.add_parser('dags',     help='(SS) Install DAGs into Airflow dags folder')
	parser_db   = subparser.add_parser('database', help='(SS) Install Auxiliar SQLite database')
	

	# ---------------------------
	# StreetSpectra DAGS Commands
	# ----------------------------

	subparser = parser_dags.add_subparsers(dest='subcommand')

	parser_dags_install = subparser.add_parser('install', help='Install StreetSpectra DAGs')
	parser_dags_install.add_argument('name',  type=str, help='Airflow DAG file to install [without .py]')
	parser_dags_install.add_argument('directory',  type=str, help='Airflow DAGs Directory where to copy')

	# -------------------------------
	# StreetSpectra Database Commands
	# -------------------------------

	subparser = parser_db.add_subparsers(dest='subcommand')

	parser_db_install = subparser.add_parser('install', help='Install StreetSpectra auxiliar SQLite database')
	parser_db_install.add_argument('name',  type=str, help='Database file to install [without .sql]')
	parser_db_install.add_argument('directory',  type=str, help='Airflow auxiliar directory where to create the database')

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
		name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
		package = f"{name}.{options.pilot}"
		command  = f"{options.command}"
		subcommand = f"{options.subcommand}"
		try: 
			command = importlib.import_module(command, package=package)
		except ModuleNotFoundError:	# when debugging module in git source tree ...
			command  = f".{options.command}"
			command = importlib.import_module(command, package=package)
		log.info(f"============== {name} {__version__} ==============")
		getattr(command, subcommand)(options)
	except KeyboardInterrupt as e:
		log.critical("[%s] Interrupted by user ", __name__)
	except Exception as e:
		log.critical("[%s] Fatal error => %s", __name__, str(e) )
		traceback.print_exc()
	finally:
		pass

main()

