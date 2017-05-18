#!/bin/bash

/opt/spark/latest/bin/spark-submit \
             --master mesos://mesos03.acis.ufl.edu:5050 \
             --driver-memory 2g \
             --total-executor-cores 48 \
             --executor-memory 1250M \
             --proxy-user hdfs \
             es_load_job_mini.py
