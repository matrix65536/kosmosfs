#!/bin/sh
#
# $Id$
#
# An endless loop that cleans up excess KFS checkpoints and logs
# and then sleeps
#
# usage: ./kfsclean.sh [-m min] [-M max] [-t time] [-s sleep] [-d dir]

me=$0
CLEANER="scripts/kfsprune.py"

# default values
sleep_time=3600
min_save=10
max_save=100
keep_time=3600
kfs_dir="."

# process any command-line arguments
TEMP=`getopt -o m:M:t:s:d:h -l min:,max:,time:,sleep:,dir:,help \
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
	-h|--help) echo "usage: $0 [-m min] [-M max] [-t time] [-s sleep] [-d dir]"; exit ;;
	--) shift; break ;;
	esac
done

if [ -f $kfs_dir/bin/metaserver ];
    then	
    cpdir="$kfs_dir/bin/kfscp"
    logdir="$kfs_dir/bin/kfslog"
    cpfile="chkpt"
    logfile="log"
elif [ -f $kfs_dir/bin/chunkserver ];
    then
    cpdir="$kfs_dir/bin/kfslog"
    logdir="$kfs_dir/bin/kfslog"
    cpfile="ckpt"
    logfile="logs"
else
    echo "No server exists...exiting"
    exit
fi

# clean up forever
while true
do
	$CLEANER -m $min_save -M $max_save -t $keep_time $cpdir $cpfile
	$CLEANER -m $min_save -M $max_save -t $keep_time $logdir $logfile
	sleep $sleep_time
done
