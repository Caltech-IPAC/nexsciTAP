# =====================================================================
#
# Makefile for product: TAP web service (including C table formatting 
# code compiled by setup.py).
#
# =====================================================================

.SUFFIXES:
INSTALL = install
INSTALL_PROGRAM = $(INSTALL) -m 755 -p
INSTALL_DIR = $(INSTALL) -m 755 -d

# The names of all the modules in this package (not including
# writerecsmodule.c that is compiled by setup.py)
#
progs = nph-tap.py adql.py configparam.py datadictionary.py \
		  runquery.py propfilter.py writeresult.py tablenames.py

# Standard compiler variables
#
CC = gcc
CFLAGS = -O -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE

# ================================================================

.PHONY :      			all installdirs install
.DELETE_ON_ERROR : ;

# Creates required installation directories. 
# CGIDIR is either an environment variable pointing
# to where you want the executable installed or you
# can add it to this Makefile as an internal variable.
#
installdirs :
							$(CGIDIR) 

# Installs web CGI executables
#
install :		      $(progs) installdirs
							rm -rf $(CGIDIR)/*
							$(INSTALL_PROGRAM) $(progs) $(CGIDIR)
							python setup.py bdist_wheel
