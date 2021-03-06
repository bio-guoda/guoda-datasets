#!/bin/bash

if grep "NotebookApp.password" ~/.jupyter/jupyter_notebook_config.py ; then

	# http://ramhiser.com/2015/02/01/configuring-ipython-notebook-support-for-pyspark/
	export SPARK_HOME="/opt/spark"
	#export PYSPARK_SUBMIT_ARGS="--master cloudera0.acis.ufl.edu"
	export PYSPARK_SUBMIT_ARGS="--executor-memory 80G --driver-memory 80G --num-executors 3 --executor-cores 5 pyspark-shell "

#--jars ${SPARK_HOME}/jars/elasticsearch-spark_2.11-2.4.0.jar
	jupyter notebook --ip='*' --no-browser

else
	echo "No password"
	echo "See https://jupyter-notebook.readthedocs.io/en/latest/public_server.html#securing-a-notebook-server"
fi
