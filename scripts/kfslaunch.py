#!/usr/bin/python

# Script that launches KFS servers on a set of nodes

# Assumes the following directory hierarchy:
# rundir/
#       - bin
#       - logs/ --> logs from running the program
#       - scripts
# Use machines.txt

import os,os.path,sys,getopt
from ConfigParser import ConfigParser

def usage():
    print "%s [-f, --file <machines.cfg>] [ [-s, --start] | [-S, --stop] ]\n" % sys.argv[0]
    
# Specify whether we want to start/stop services
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "f:sSh",
                                 ["file=", "start", "stop", "help"])
    op = ""
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-s", "--start"):
            op = "--start"
        elif o in ("-S", "--stop"):
            op = "--stop"

    if op == "":
        raise getopt.GetoptError, "invalid options"

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"
    
    sections = config.sections()

    for s in sections:
        node = config.get(s, 'node')
        rundir = config.get(s, 'rundir')
        if (s == 'metaserver'):
            runargs = "-m -f bin/MetaServer.prp"
        else:
            runargs = "-c -f bin/ChunkServer.prp"
            
        cmd = "ssh %s 'cd %s; sh scripts/kfsrun.sh %s %s ' " % \
              (node, rundir, op, runargs)
        print cmd
        os.system(cmd)
    
