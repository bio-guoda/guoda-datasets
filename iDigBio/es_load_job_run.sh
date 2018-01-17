#!/bin/bash

export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HADOOP_USER_NAME="hdfs"

/opt/spark/latest/bin/spark-submit \
             --master mesos://zk://mesos02:2181,mesos01:2181,mesos03:2181/mesos \
             --driver-memory 2G \
             --total-executor-cores 48 \
             --executor-memory 20G \
             --conf spark.sql.shuffle.partitions=3000 \
             es_load_job.py
