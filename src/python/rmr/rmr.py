#
# $Id: //depot/SOURCE/OPENSOURCE/kfs/src/python/rmr/rmr.py#1 $
#
# do a recursive remove on a KFS directory
#
import kfs
from readdirplus import *
from stat import *

def rmr(client, path):
	if S_ISREG(client.stat(path)[ST_MODE]):
		client.remove(path)
	else:
		do_rmr(client, path)

def do_rmr(client, path):
	rdp = client.readdirplus(path)
	dot = [r for r in rdp if rd_name(r) == '.']
	my_id = rd_id(dot[0])
	subs = [r for r in rdp if rd_isdir(r) and rd_id(r) != my_id and \
			rd_name(r) != '..']
	plain = [r for r in rdp if rd_isreg(r)]
	for r in plain:
		client.remove(path + "/" + rd_name(r))
	for r in subs:
		do_rmr(client, path + "/" + rd_name(r))
	client.rmdir(path)
