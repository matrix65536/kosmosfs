#
# $Id: //depot/SOURCE/OPENSOURCE/kfs/src/python/flogger/readdirplus.py#1 $
# KFS readdirplus definitions
#
"""
A little module that defines offsets and a few simple functions
for accessing KFS readdirplus data, in the spirit of the standard
'stat' module.
"""

RD_NAME = 0
RD_FILEID = 1
RD_MTIME = 2
RD_CTIME = 3
RD_CRTIME = 4
RD_TYPE = 5
RD_SIZE = 6

def rd_isreg(rdtuple):
	return rdtuple[RD_TYPE] == "file"

def rd_isdir(rdtuple):
	return rdtuple[RD_TYPE] == "dir"

def rd_name(rdtuple):
	return rdtuple[RD_NAME]

def rd_size(rdtuple):
	return rdtuple[RD_SIZE]

def rd_id(rdtuple):
	return rdtuple[RD_FILEID]
