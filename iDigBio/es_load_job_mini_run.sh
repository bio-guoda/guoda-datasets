#!/bin/bash

spark-submit --master mesos://mesos01.acis.ufl.edu:5050 --driver-memory 50g es_load_job_mini.py
