#!/usr/bin/python
#
# $Id: kfssetup.py 36 2007-11-12 02:43:36Z sriramsrao $
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
import socket,threading
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
tarProg = 'gtar'
maxConcurrent = 25

def setupMeta(section, config, outputFn, packageFn):
    """ Setup the metaserver binaries/config files on a node. """
    fh = open(outputFn, 'w')
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
    if config.has_option(section, 'loglevel'):
        s = "metaServer.loglevel = %s\n" % config.get(section, 'loglevel')
        fh.write(s)
    
    if config.has_option(section, 'numservers'):
        n = config.get(section, 'numservers')
        s = "metaServer.minChunkservers = %s" % n
        fh.write(s)
    fh.close()
    cmd = "%s -zcf %s bin/logcompactor bin/metaserver %s lib scripts/*" % (tarProg, packageFn, outputFn)
    os.system(cmd)
    installArgs = "-d %s -m" % (rundir)
    return installArgs    

def setupChunk(section, config, outputFn, packageFn):
    """ Setup the chunkserver binaries/config files on a node. """    
    metaNode = config.get('metaserver', 'node')
    metaToChunkPort = config.getint('metaserver', 'baseport') + 100
    hostname = config.get(section, 'node')
    # for rack-aware replication, we assume that nodes on different racks are on different subnets
    s = socket.gethostbyname(hostname)
    ipoctets = s.split('.')
    rackId = int(ipoctets[2])
    fh = open (outputFn, 'w')
    s = "chunkServer.metaServer.hostname = %s\n" % metaNode
    fh.write(s)
    s = "chunkServer.metaServer.port = %d\n" % metaToChunkPort
    fh.write(s)
    s = "chunkServer.clientPort = %d\n" % config.getint(section, 'baseport')
    fh.write(s)
    key = config.get('metaserver', 'clusterkey')
    s = "chunkServer.clusterKey = %s\n" % (key)
    fh.write(s)
    s = "chunkServer.rackId = %d\n" % (rackId)
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

    if config.has_option(section, 'loglevel'):
        s = "chunkServer.loglevel = %s\n" % config.get(section, 'loglevel')
        fh.write(s)
        
    fh.close()
    cmd = "%s -zcf %s bin/chunkupgrade bin/chunkscrubber bin/chunkserver %s lib scripts/*" % (tarProg, packageFn, outputFn)
    os.system(cmd)
    installArgs = "-d %s -c \"%s\" " % (rundir, chunkDir)
    return installArgs

def usage():
    """ Print out the usage for this program. """
    print "%s [-f, --file <machines.cfg>] [-t, --tar <tar|gtar>] [ [-b, --bin <dir with binaries>] {-u, --upgrade} | [-U, --uninstall] ]\n" \
          % sys.argv[0]
    return

def copyDir(srcDir, dstDir):
    """ Copy files from src to dest"""
    cmd = "cp -r %s %s" % (srcDir, dstDir)
    os.system(cmd)

    
def getFiles(buildDir):
    """ Copy files from buildDir/bin, buildDir/lib and . to ./bin, ./lib, and ./scripts
    respectively."""

    cmd = "mkdir -p ./scripts; cp ./* scripts; chmod u+w scripts/*"
    os.system(cmd)
    s = "%s/bin" % buildDir
    copyDir(s, './bin')
    s = "%s/lib" % buildDir
    copyDir(s, './lib')

def cleanup():
    """ Cleanout the dirs we created. """
    cmd = "rm -rf ./scripts"
    os.system(cmd)
    cmd = "rm -rf ./bin"
    os.system(cmd)
    cmd = "rm -rf ./lib"
    os.system(cmd)

class InstallWorker(threading.Thread):
    """InstallWorker thread that runs a command on remote node"""
    def __init__(self, sec, conf, tmpdir, i, m):
        threading.Thread.__init__(self)
        self.section = sec
        self.config = conf
        self.tmpdir = tmpdir
        self.id = i
        self.mode = m

    def buildPackage(self):
        if (self.section == 'metaserver'):
            self.installArgs = setupMeta(self.section, self.config, self.configOutputFn, self.packageFn)
        else:
            self.installArgs = setupChunk(self.section, self.config, self.configOutputFn, self.packageFn)

    def doInstall(self):
        fn = os.path.basename(self.packageFn)
        c = "scp -q %s kfsinstall.sh %s:/tmp/; ssh %s 'mv /tmp/%s /tmp/kfspkg.tgz; sh /tmp/kfsinstall.sh %s %s ' " % \
            (self.packageFn, self.dest, self.dest, fn, self.mode, self.installArgs)
        os.system(c)
        
    def cleanup(self):
        c = "rm -f %s %s" % (self.configOutputFn, self.packageFn)
        os.system(c)
        c = "ssh %s 'rm -f /tmp/install.sh /tmp/kfspkg.tgz' " % self.dest
        os.system(c)
        
    def run(self):
        self.configOutputFn = "%s/fn.%d" % (self.tmpdir, self.id)
        self.packageFn = "%s/kfspkg.%d.tgz" % (self.tmpdir, self.id)
        self.dest = config.get(self.section, 'node')
        self.buildPackage()
        self.doInstall()
        self.cleanup()
        
def doInstall(config, builddir, tmpdir, upgrade, serialMode):
    if not config.has_section('metaserver'):
        raise config.NoSectionError, "No metaserver section"

    if not os.path.exists(builddir):
        print "%s : directory doesn't exist\n" % builddir
        sys.exit(-1)

    getFiles(builddir)

    workers = []
    i = 0
    sections = config.sections()
    if upgrade == 1:
        mode = "-u"
    else:
        mode = "-i"
    
    for s in sections:
        w = InstallWorker(s, config, tmpdir, i, mode)
        workers.append(w)
        if serialMode == 1:
            w.start()
            w.join()
        i = i + 1

    if serialMode == 0:
        for i in xrange(len(workers)):
            #start a bunch 
            for j in xrange(maxConcurrent):
                idx = i * maxConcurrent + j
                if idx >= len(workers):
                    break
                workers[idx].start()
            #wait for each one to finish
            for j in xrange(maxConcurrent):
                idx = i * maxConcurrent + j
                if idx >= len(workers):
                    break
                workers[idx].join()
                
            
    print "Started all the workers..waiting for them to finish"
    for i in xrange(len(workers)):
        workers[i].join(120.0)
        
    cleanup()

class UnInstallWorker(threading.Thread):
    """UnInstallWorker thread that runs a command on remote node"""
    def __init__(self, c):
        threading.Thread.__init__(self)
        self.cmd = c
    def run(self):
        print "Running cmd %s" % (self.cmd)
        os.system(self.cmd)

def doUninstall(config):
    sections = config.sections()
    workers = []

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
        # print "Uninstall cmd: %s\n" % cmd
        # os.system(cmd)
        w = UnInstallWorker(cmd)
        workers.append(w)
        w.start()

    print "Started all the workers..waiting for them to finish"        
    for i in xrange(len(workers)):
        workers[i].join(120.0)
    sys.exit(0)
    
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "b:f:r:t:hsUu",
                                 ["build=", "file=", "tar=", "tmpdir=", "help", "serialMode", "uninstall", "upgrade"])
    filename = ""
    builddir = ""
    uninstall = 0
    upgrade = 0
    serialMode = 0
    # Script probably won't work right if you change tmpdir from /tmp location
    tmpdir = "/tmp"
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-b", "--build"):
            builddir = a
        elif o in ("-r", "--tar"):
            tarProg = a
        elif o in ("-t", "--tmpdir"):
            tmpdir = a
        elif o in ("-U", "--uninstall"):
            uninstall = 1
        elif o in ("-u", "--upgrade"):
            upgrade = 1
        elif o in ("-s", "--serialMode"):
            serialMode = 1

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))

    if uninstall == 1:
        doUninstall(config)
    else:
        doInstall(config, builddir, tmpdir, upgrade, serialMode)
        
