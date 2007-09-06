#!/usr/bin/python

# Script to setup KFS servers on a set of nodes
# This scripts reads a machines.cfg file that describes the meta/chunk
# servers configurations and installs the binaries/scripts and creates
# the necessary directory hierarchy.

import os,sys,os.path,getopt
from ConfigParser import ConfigParser

# Use the python config parser to parse out machines setup
# Input file format for machines.cfg
# [metaserver]
#   type: metaserver
#   node: <value>
#   rundir: <dir>
#   baseport: <port>
#
# [chunkserver1]
#   node: <value>
#   rundir: <dir>
#   baseport: <port>
#   space: <space in bytes>
#   {chunkdir: <dir>}
# [chunkserver2]
# ...
# [chunkserverN]
# ...
#
# Install on each machine with the following directory hierarchy:
#   rundir/
#        bin/  -- binaries, config file, kfscp/kfslog/kfschunk dirs
#        logs/ -- log output from running the binary
#        scripts/ -- all the helper scripts
# If a path for storing the chunks isn't specified, then it defaults to bin
#


def setupMeta(section, config):
    """ Setup the metaserver binaries/config files on a node. """
    fh = open('bin/MetaServer.prp', 'w')
    baseport = config.getint(section, 'baseport')
    s = "metaServer.clientPort = %d\n" % baseport
    fh.write(s)
    s = "metaServer.chunkServerPort = %d\n" % (baseport + 100)
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
    s = "chunkServer.totalSpace = %s\n" % config.get(section, 'space')
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
    installArgs = "-d %s -c %s" % (rundir, chunkDir)
    return installArgs

def usage():
    """ Print out the usage for this program. """
    print "%s [-f, --file <machines.cfg>] [ [-b, --bin <dir with binaries>] | [-u, --uninstall] ]\n" % sys.argv[0]
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

def doInstall(config, bindir):
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
        cmd = "scp -q kfspkg.tgz kfsinstall.sh %s:/tmp; ssh %s 'sh /tmp/kfsinstall.sh -i %s ' " % \
              (node, node, installArgs)
        print "Install cmd: %s\n" % cmd
        os.system(cmd)
        os.remove('kfspkg.tgz')
        # Cleanup remote
        cmd = "ssh %s 'rm -f /tmp/kfsinstall.sh kfspkg.tgz' " % (node)
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
            otherArgs = "-c %s" % (chunkDir)
        
        cmd = "ssh %s 'sh %s/scripts/kfsinstall.sh -U -d %s %s' " % \
              (node, rundir, rundir, otherArgs)
        print "Uninstall cmd: %s\n" % cmd
        os.system(cmd)
    
if __name__ == '__main__':
    (opts, args) = getopt.getopt(sys.argv[1:], "b:f:hu",
                                 ["bin=", "file=", "help", "uninstall"])
    filename = ""
    bindir = ""
    uninstall = 0
    for (o, a) in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(2)
        if o in ("-f", "--file"):
            filename = a
        elif o in ("-b", "--bin"):
            bindir = a
        elif o in ("-u", "--uninstall"):
            uninstall = 1

    if not os.path.exists(filename):
        print "%s : directory doesn't exist\n" % filename
        sys.exit(-1)

    config = ConfigParser()
    config.readfp(open(filename, 'r'))

    if uninstall == 1:
        doUninstall(config)
    else:
        doInstall(config, bindir)
        
