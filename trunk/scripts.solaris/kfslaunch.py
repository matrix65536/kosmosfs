#!/usr/bin/python
#
# $Id: kfslaunch.py 36 2007-11-12 02:43:36Z sriramsrao $
#
# Copyright 2007 Kosmix Corp.
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# Script that launches KFS servers on a set of nodes
#
# Assumes the following directory hierarchy:
# rundir/
#       - bin
#       - logs/ --> logs from running the program
#       - scripts
# Use machines.cfg
#

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
            op = "-s"
        elif o in ("-S", "--stop"):
            op = "-S"

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
            if config.has_option(s, 'backup_path'):
                bkup_path = config.get(s, 'backup_path')
                runargs = runargs + " -b %s" % (bkup_path)
        else:
            runargs = "-c -f bin/ChunkServer.prp"
            
        cmd = "ssh %s 'cd %s; scripts/kfsrun.sh %s %s ' " % \
              (node, rundir, op, runargs)
        print cmd
        os.system(cmd)
    
