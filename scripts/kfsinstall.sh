#!/bin/bash
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
# Script to install/upgrade a server package on a machine
#

installServer()
{
    # if everything exists, return
    if [ -d $serverDir ] && [ -f $serverDir/bin/$serverBinary ] ;
	then
	    echo "$serverBinary exists...skipping"
    fi

    if [ ! -d $serverDir ]; 
	then
	mkdir -p $serverDir
    fi

    if [ ! -d $serverDir ]; 
	then
	echo "Unable to create $serverDir"
	exit -1
    fi

    cd $serverDir; tar -zxf /tmp/kfspkg.tgz 

    # Make a logs dir for the startup script
    mkdir -p $serverDir/logs

    case $serverType in
	-m|--meta) 
	    mkdir -p $serverDir/bin/kfscp
	    mkdir -p $serverDir/bin/kfslog
	    ;;
	-c|--chunk)
	    mkdir -p $serverDir/bin/kfslog
	    ;;
	*)
	    echo "Unknown server"
	    ;;
    esac
    RETVAL=0
    return $RETVAL
}

upgradeServer()
{
    sh $serverDir/scripts/kfsrun.sh --stop $serverType 
    cp /tmp/$serverBinary $serverDir/bin
    RETVAL=0
    return $RETVAL
}

uninstallServer()
{
    sh $serverDir/scripts/kfsrun.sh --stop $serverType 

    case $serverType in
	-m|--meta) 
	    rm -rf $serverDir
	    ;;
	-c|--chunk)
	    rm -rf $chunkDir
	    rm -rf $serverDir
	    ;;
	*)
	    echo "Unknown server"
	    ;;
    esac

}

# Process any command line arguments
TEMP=`getopt -o d:mc:hiuU -l dir:,meta,chunk:,help,install,upgrade,uninstall \
	-n kfsinstall.sh -- "$@"`
eval set -- "$TEMP"

while true
  do
  case "$1" in
      -d|--dir) serverDir=$2; shift;;
      -m|--meta) serverType="$1"; serverBinary="metaserver";;
      -c|--chunk) 
	  serverType="$1"; 
	  chunkDir=$2;
	  serverBinary="chunkserver";
	  shift;;
      -i|--install) mode="install";;
      -u|--upgrade) mode="upgrade";;
      -U|--uninstall) mode="uninstall";;
      -h|--help) 
	  echo -n "usage: $0 [--serverDir <dir>] [--meta | --chunk <chunk dir>]";
	  echo " [--install | --upgrade | --uninstall ]";
	  exit;;
      --) break ;;
  esac
  shift
done

case $mode in
    "install")
	installServer
	;;
    "upgrade")
	upgradeServer
	;;
    "uninstall")
	uninstallServer
	;;
    *)
	echo "Need to specify mode"
	exit 1
esac


exit $RETVAL
