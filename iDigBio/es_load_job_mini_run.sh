#!/bin/bash

export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HADOOP_USER_NAME="hdfs"

spark-submit --master mesos://zk://mesos02:2181,mesos01:2181,mesos03:2181/mesos \
             --driver-memory 2g \
             --total-executor-cores 48 \
             --executor-memory 1250M \
             es_load_job_mini.py
