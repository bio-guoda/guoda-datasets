#!/bin/bash
# 
# Download and import OTT taxon graph into GUODA hdfs .
#

set -x

VERSION=3.0
IMPORT_DATE=20170226

wget http://files.opentreeoflife.org/ott/ott${VERSION}/ott${VERSION}.tgz
tar xvf ott${VERSION}.tgz

hdfs dfs -mkdir -p /guoda/data/source=ott/date=${IMPORT_DATE}
hdfs dfs -copyFromLocal ott/taxonomy.tsv /guoda/data/source=ott/date=${IMPORT_DATE}/
