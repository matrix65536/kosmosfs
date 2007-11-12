#!/bin/sh
#
# $Id$
#
# Copyright 2006 Kosmix Corp.
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
#
# An endless loop that cleans up excess KFS checkpoints and logs
# and then sleeps; also, for metaserver, if a backup location is
# specified, then the checkpoint files are backed up.
#
# usage: ./kfsclean.sh [-m min] [-M max] [-t time] [-s sleep] [-d dir]
# {[-r remote] [-p remote_path]}

me=$0
CLEANER="scripts/kfsprune.py"

# default values
sleep_time=3600
min_save=10
max_save=100
keep_time=3600
kfs_dir="."

# process any command-line arguments
TEMP=`getopt -o m:M:t:s:d:b:h -l min:,max:,time:,sleep:,dir:,backup:,help \
		-n kfsclean.sh -- "$@"`
eval set -- "$TEMP"

while true
do
	case "$1" in
	-m|--min) min_save=$2; shift 2 ;;
	-M|--max) max_save=$2; shift 2 ;;
	-t|--time) keep_time=$2; shift 2 ;;
	-s|--sleep) sleep_time=$2; shift 2 ;;
	-d|--dir) kfs_dir=$2; shift 2 ;;
	-b|--backup) backup_path=$2; shift 2 ;;
	-h|--help) echo "usage: $0 [-m min] [-M max] [-t time] [-s sleep] [-d dir] {[-b backup]}"; exit ;;
	--) shift; break ;;
	esac
done

if [ -f $kfs_dir/bin/metaserver ];
    then	
    cpdir="$kfs_dir/bin/kfscp"
    logdir="$kfs_dir/bin/kfslog"
    cpfile="chkpt"
    logfile="log"
    metabkup="$kfs_dir/scripts/metabkup.sh"
    chmod a+x $metabkup
elif [ -f $kfs_dir/bin/chunkserver ];
    then
    cpdir="$kfs_dir/bin/kfslog"
    logdir="$kfs_dir/bin/kfslog"
    cpfile="ckpt"
    logfile="logs"
    metabkup=
else
    echo "No server exists...exiting"
    exit
fi

# clean up forever
while true
do
	echo " `date` : Cleaning cp/logs" 
	$CLEANER -m $min_save -M $max_save -t $keep_time $cpdir $cpfile
	$CLEANER -m $min_save -M $max_save -t $keep_time $logdir $logfile
	if [ -e $metabkup ];
	    then
	    echo " `date` : Backing up metaserver checkpoints to: $backup_path"
	    $metabkup -d $cpdir -b $backup_path
	fi
	sleep $sleep_time
done
