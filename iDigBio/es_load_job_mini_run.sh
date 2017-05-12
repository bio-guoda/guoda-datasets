#!/bin/bash

spark-submit --master mesos://mesos01.acis.ufl.edu:5050 \
             --driver-memory 2g \
             --total-executor-cores 48 \
             --executor-memory 1250M \
             es_load_job_mini.py
