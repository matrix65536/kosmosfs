#!/bin/sh
#
# $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/scripts/kfsclean.sh#1 $
#
# An endless loop that cleans up excess KFS checkpoints and logs
# and then sleeps
#
# usage: ./metaclean.sh [-m min] [-M max] [-t time] [-s sleep] [-d dir]

me=$0
CLEANER="./metaprune.py"

# default values
sleep_time=3600
min_save=10
max_save=100
keep_time=3600
kfs_dir="."

# process any command-line arguments
TEMP=`getopt -o m:M:t:s:d:h -l min:,max:,time:,sleep:,dir:,help \
		-n metaclean.sh -- "$@"`
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
	
cpdir="$kfs_dir/kfscp"
logdir="$kfs_dir/kfslog"

# clean up forever
while true
do
	$CLEANER -m $min_save -M $max_save -t $keep_time $cpdir chkpt
	$CLEANER -m $min_save -M $max_save -t $keep_time $logdir log
	sleep $sleep_time
done
