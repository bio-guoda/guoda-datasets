#!/bin/bash
#
#  Start a spark environment and say hi.
#

#source /etc/hadoop/conf/hadoop-env.sh
/opt/spark/latest/bin/spark-shell --master mesos://zk://mesos01:2182,mesos02:2181,mesos03:2181/mesos --driver-memory 4G --executor-memory 4G < println ("hello world")

