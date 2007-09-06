#/bin/bash

# Script to start/stop a meta/chunk server on a node
# 
#
startServer()
{
    if [ -e $SERVER_PID_FILE ];
    then
	echo "$server is already running..."
	exit 0
    fi

    if [ ! -f $config ];
    then
	echo "No config file...Not starting $server"
	exit -1
    fi
    echo -n $"Starting $server"
    bin/$server $config > $SERVER_LOG_FILE < /dev/null 2>&1 &
    echo $! > $SERVER_PID_FILE

    if [ ! -e $CLEANER_PID_FILE ];
	then
	sh scripts/kfsclean.sh > $CLEANER_LOG_FILE < /dev/null 2>&1 &
	echo $! > $CLEANER_PID_FILE
    else
	echo "cleaner is already running..."
    fi

    RETVAL=$?
    echo 
    return $RETVAL
}

stopServer()
{
    echo -n $"Stopping $server"

    if [ ! -e $PID_FILE ]; 
	then
	echo "ERROR: No PID file ( $PID_FILE )"
	return -1;
    fi;

    PROCID=`cat $PID_FILE`
    if [ -z $PROCID ]; 
	then
	echo ERROR: No PID value in file
	return -2;
    fi

    PROC_COUNT=`ps -ef | awk '{print $2}'  | grep -c $PROCID`
    if [[ $PROC_COUNT -gt  0 ]]; 
	then
	echo -n $"Stopping $prog ( $PROCID )"
	kill -TERM $PROCID
    fi;

    rm -f $PID_FILE

    echo
    RETVAL=$?
    return $RETVAL
}

# Process any command line arguments
TEMP=`getopt -o f:sSmch -l file:,start,stop,meta,chunk,help \
	-n kfsrun.sh -- "$@"`
eval set -- "$TEMP"

while true
do
	case "$1" in
	-s|--start) mode="start";;
	-S|--stop) mode="stop";;
	-m|--meta) server="metaserver";;
	-c|--chunk) server="chunkserver";;
	-f|--file) config=$2; shift;;
	-h|--help) echo "usage: $0 [--start | --stop] [--meta | --chunk] [--file <config>"; exit;;
	--) break ;;
	esac
	shift
done

[ -f bin/$server ] || exit 0
LOGS_DIR="logs"
SERVER_LOG_FILE=$LOGS_DIR/$server.log
SERVER_PID_FILE=$LOGS_DIR/$server.pid

CLEANER_LOG_FILE=$LOGS_DIR/$server.cleaner.log
CLEANER_PID_FILE=$LOGS_DIR/$server.cleaner.pid

case $mode in
    "start")
	startServer
	;;
    "stop")
	PID_FILE=$SERVER_PID_FILE
	stopServer
	PID_FILE=$CLEANER_PID_FILE
	stopServer
	;;
    *)
	echo "Need to specify server"
	exit 1
esac


exit $RETVAL
