#!/bin/bash

SCRIPT_NAME="$(basename $0)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

p_unknown () {
  echo "Parameter error: $1"
  exit 1
}

usage () {
  cat << EOF

This script start runnig for calculating attribution from events and impressions data

Usage:
  $SCRIPT_NAME  [--partition partition] [--driver_memory driver_memory] [--executor_memory executor_memory] \
  [--executor_cores executor_cores] [--num_executors num_executors] [--events_input_dir events_input_dir] \
  [--impressions_input_dir impressions_input_dir] [--output_dir output_dir] [--app_env app_env]

Options:
 -h
  Print help screen
 --partition
  The number of partition number for spark application (ex: 100)
 --driver_memory
  The driver memory for spark application (ex: 4G)
 --executor_memory
  The executor memory for spark application (ex: 4G)
 --executor_cores
  The core number assigned to each executor (ex: 2)
 --num_executors
  Total executor number for the spark application (ex: 100)
 --events_input_dir
  The input directory of events file (if application is in the cluster environment, 
  the path should be hdfs path)
 --impressions_input_dir
  The input directory of impressions file (if application is in the cluster environment, 
  the path should be hdfs path)
 --output_dir
  The output directory of generated result (if application is in the cluster environment, 
  the path should be hdfs path)
 --app_env
  In local machine: (app_env=dev) 
  In cluster environment: (app_env=qa) or (app_env=prod)

EOF
exit 0
}

while [ "$1" ]; do
  case "$1" in
     -h) usage; shift;;
     --partition)
       if [ -n "$2" ] ; then
         partition="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
     --driver_memory)
       if [ -n "$2" ] ; then
         driver_memory="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
     --executor_memory)
       if [ -n "$2" ] ; then
         executor_memory="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
     --executor_cores)
       if [ -n "$2" ] ; then
         executor_cores="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
     --num_executors)
       if [ -n "$2" ] ; then
         num_executors="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
    --events_input_dir)
       if [ -n "$2" ] ; then
         events_input_dir="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
    --impressions_input_dir)
       if [ -n "$2" ] ; then
         impressions_input_dir="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
    --output_dir)
       if [ -n "$2" ] ; then
         output_dir="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
    --app_env)
       if [ -n "$2" ] ; then
         app_env="$2"
         shift 2
       else
         p_unknown "missing argument, see $SCRIPT_NAME -h for help"
       fi
       ;;
     --) shift ; break;;
     *)  p_unknown "Internal error";;
  esac
done

if [ -z "$partition" ] || [ -z "$driver_memory" ] || [ -z "$executor_memory" ] || [ -z "$executor_cores" ] || [ -z "$num_executors" ] || [ -z "$events_input_dir" ] || [ -z "$impressions_input_dir" ] || [ -z "$output_dir" ]; then
    p_unknown "Unknown/invalid argument, see $SCRIPT_NAME -h for help"
fi

if [ "$app_env" != "dev" ] || [ ! "$app_env" != "qa" ] || [ ! "$app_env" != "prod" ]; then
    p_unknown "Unknown/invalid app_env argument, see $SCRIPT_NAME -h for help"  
fi

SCRIPT_LOCATION="$SCRIPT_DIR/calculate_attribution.py"
PROCESSED_DIR="$output_dir/processed"
OUTPUT_TYPES=( "count_of_events" "count_of_users" )

mkdir -p $PROCESSED_DIR

echo "`date`: start calculating attribution"

$SPARK_HOME/bin/spark-submit \
--driver-memory $driver_memory --executor-memory $executor_memory \
--executor-cores $executor_cores --num-executors $num_executors \
$SCRIPT_LOCATION \
--events_input_dir=$events_input_dir --impressions_input_dir=$impressions_input_dir \
--output_dir=$output_dir --num_partitions=$partition

if [ "$app_env" == "dev" ]; then
    for data_type in "${OUTPUT_TYPES[@]}"; do
        if [ -f "$output_dir/$data_type/_SUCCESS" ]; then
            cat $output_dir/$data_type/part* > $PROCESSED_DIR/$data_type.csv
            rm -r $output_dir/$data_type
        fi
    done
else
    for data_type in "${OUTPUT_TYPES[@]}"; do
        hdfs dfs -test -f "$output_dir/$data_type/_SUCCESS"
        if [ $? == 0 ]; then
            hdfs dfs -getmerge $output_dir/$data_type/part* $PROCESSED_DIR/$data_type.csv
            hdfs dfs -rmr $output_dir/$data_type
        fi
    done
fi

echo "completed attribution exporting at `date`"
