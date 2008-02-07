#!/usr/bin/python
#
# $Id$
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
# Script to setup KFS servers on a set of nodes
# This scripts reads a machines.cfg file that describes the meta/chunk
# servers configurations and installs the binaries/scripts and creates
# the necessary directory hierarchy.
#

import os,sys,os.path,getopt
from ConfigParser import ConfigParser

# Use the python config parser to parse out machines setup
# Input file format for machines.cfg
# [metaserver]
#   type: metaserver
#   clusterkey: <cluster name>
#   node: <value>
#   rundir: <dir>
#   baseport: <port>
#
# [chunkserver1]
#   node: <value>
#   rundir: <dir>
#   baseport: <port>
#   space: <space exported by the server> (n m/g)
#   {chunkdir: <dir>}
# [chunkserver2]
# ...
# [chunkserverN]
# ...
#
# where, space is expressed in units of MB/GB or bytes.
#
# Install on each machine with the following directory hierarchy:
#   rundir/
#        bin/  -- binaries, config file, kfscp/kfslog/kfschunk dirs
#        logs/ -- log output from running the binary
#        scripts/ -- all the helper scripts
# If a path for storing the chunks isn't specified, then it defaults to bin
#

unitsScale = {'g' : 1 << 30, 'm' : 1 << 20, 'k' : 1 << 10, 'b' : 1}

def setupMeta(section, config):
    """ Setup the metaserver binaries/config files on a node. """
    fh = open('bin/MetaServer.prp', 'w')
    baseport = config.getint(section, 'baseport')
    s = "metaServer.clientPort = %d\n" % baseport
    fh.write(s)
    s = "metaServer.chunkServerPort = %d\n" % (baseport + 100)
    fh.write(s)
    key = config.get(section, 'clusterkey')
    s = "metaServer.clusterKey = %s\n" % (key)
    fh.write(s)
    rundir = config.get(section, 'rundir')
    s = "metaServer.cpDir = %s/bin/kfscp\n" % rundir
    fh.write(s)
    s = "metaServer.logDir = %s/bin/kfslog\n" % rundir
    fh.write(s)
    fh.close()
    cmd = "tar -zcf kfspkg.tgz bin/metaserver bin/MetaServer.prp scripts/*"
    os.system(cmd)
    installArgs = "-d %s -m" % (rundir)
    return installArgs    

def setupChunk(section, config):
    """ Setup the chunkserver binaries/config files on a node. """    
    metaNode = config.get('metaserver', 'node')
    metaToChunkPort = config.getint('metaserver', 'baseport') + 100
    fh = open ('bin/ChunkServer.prp', 'w')
    s = "chunkServer.metaServer.hostname = %s\n" % metaNode
    fh.write(s)
    s = "chunkServer.metaServer.port = %d\n" % metaToChunkPort
    fh.write(s)
    s = "chunkServer.clientPort = %d\n" % config.getint(section, 'baseport')
    fh.write(s)
    key = config.get('metaserver', 'clusterkey')
    s = "chunkServer.clusterKey = %s\n" % (key)
    fh.write(s)
    space = config.get(section, 'space')
    s = space.split()
    if (len(s) >= 2):
        units = s[1].lower()
    else:
        units = 'b'
    
    value = int(s[0]) * unitsScale[ units[0] ]
    s = "chunkServer.totalSpace = %d\n" % value
    fh.write(s)
    rundir = config.get(section, 'rundir')
    if config.has_option(section, 'chunkdir'):
        chunkDir = config.get(section, 'chunkdir')
    else:
        chunkDir = "%s/bin/kfschunk" % (rundir)

    s = "chunkServer.chunkDir = %s\n" % (chunkDir)
    fh.write(s)
    s = "chunkServer.logDir = %s/bin/kfslog\n" % (rundir)
    fh.write(s)
    fh.close()
    cmd = "tar -zcf kfspkg.tgz bin/chunkserver bin/ChunkServer.prp scripts/*"
    os.system(cmd)
    installArgs = "-d %s -c \"%s\" " % (rundir, chunkDir)
    return installArgs

def usage():
    """ Print out the usage for this program. """
    print "%s [-f, --file <machines.cfg>] [ [-b, --bin <dir with binaries>] {-u, --upgrade} | [-U, --uninstall] ]\n" \
          % sys.argv[0]
    return

def copyDir(srcDir, dstDir):
    """ Copy files from src to dest; make the dest dir if it doesn't
    exist"""
    files = os.listdir(srcDir)
    if not os.path.exists(dstDir):
        os.mkdir(dstDir)
    for f in files:
        cmd = 'cp %s/%s %s' % (srcDir, f, dstDir)
        os.system(cmd)

    
def getFiles(bindir):
    """ Copy files from bin and . to ./bin and ./scripts
    respectively."""

    copyDir('.', './scripts')
    cmd = "chmod u+w scripts/*"
    os.system(cmd)
    copyDir(bindir, './bin')

def cleanup():
    """ Cleanout the dirs we created. """
    cmd = "rm -rf ./scripts"
    os.system(cmd)
    cmd = "rm -rf ./bin"
    os.system(cmd)

def doInstall(config, bindir, upgrade):
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"

    if not os.path.exists(bindir):
        print "%s : directory doesn't exist\n" % bindir
        sys.exit(-1)

    getFiles(bindir)
    
    sections = config.sections()
    for s in sections:
        if (s == 'metaserver'):
            installArgs = setupMeta(s, config)
        else:
            installArgs = setupChunk(s, config)
        node = config.get(s, 'node')
        if upgrade == 1:
            mode = "-u"
        else:
            mode = "-i"
        cmd = "scp -q kfspkg.tgz kfsinstall.sh %s:/tmp; ssh %s 'sh /tmp/kfsinstall.sh %s %s ' " % \
              (node, node, mode, installArgs)
        print "Install cmd: %s\n" % cmd
        os.system(cmd)
        os.remove('kfspkg.tgz')
        # Cleanup remote
        cmd = "ssh %s 'rm -f /tmp/kfsinstall.sh /tmp/kfspkg.tgz' " % (node)
        os.system(cmd)
        
    cleanup()

def doUninstall(config):
    sections = config.sections()
    for s in sections:
        rundir = config.get(s, 'rundir')
        node = config.get(s, 'node')
        
        if (s == 'metaserver'):
            otherArgs = '-m'
        else:
            # This is a chunkserver; so nuke out chunk dir as well
            if config.has_option(s, 'chunkdir'):
                chunkDir = config.get(s, 'chunkdir')
            else:
                chunkDir = "%s/bin/kfschunk" % (rundir)
            otherArgs = "-c \"%s\"" % (chunkDir)
        
        cmd = "ssh %s 'cd %s; sh scripts/kfsinstall.sh -U -d %s %s' " % \
              (node, rundir, rundir, otherArgs)
        print "Uninstall cmd: %s\n" % cmd
        os.system(cmd)
    
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "b:f:hUu",
                                 ["bin=", "file=", "help", "uninstall", "upgrade"])
    filename = ""
    bindir = ""
    uninstall = 0
    upgrade = 0
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-b", "--bin"):
            bindir = a
        elif o in ("-U", "--uninstall"):
            uninstall = 1
        elif o in ("-u", "--upgrade"):
            upgrade = 1

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))

    if uninstall == 1:
        doUninstall(config)
    else:
        doInstall(config, bindir, upgrade)
        
