#!/bin/bash
#
# Download and import wikidata into hdfs .
#

set -x

wget https://zenodo.org/record/1211767/files/wikidata20171227.json.bz2
hdfs dfs -put latest-all.json.bz2 hdfs://guoda/data/source=wikidata/date=20171227/latest.all.json.bz2


