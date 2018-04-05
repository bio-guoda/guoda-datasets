#!/bin/bash
# 
# Download and import GloBI taxon graph into GUODA hdfs .
#

install() {
 local ZENODO_ID=$1
 local IMPORT_DATE=$2
 wget https://zenodo.org/record/${ZENODO_ID}/files/taxonCache.tsv.gz
 wget https://zenodo.org/record/${ZENODO_ID}/files/taxonMap.tsv.gz
 zcat taxonCache.tsv.gz | bzip2 > taxonCache.tsv.bz2
 zcat taxonMap.tsv.gz | bzip2 > taxonMap.tsv.bz2

 hdfs dfs -mkdir -p /guoda/data/source=globi/date=${IMPORT_DATE}
 hdfs dfs -copyFromLocal taxonCache.tsv.bz2 /guoda/data/source=globi/date=${IMPORT_DATE}/
 hdfs dfs -copyFromLocal taxonMap.tsv.bz2 /guoda/data/source=globi/date=${IMPORT_DATE}/

 # remove intermediate files
 rm taxonCache.tsv.gz taxonMap.tsv.gz taxonMap.tsv.bz2 taxonCache.tsv.bz2
}

install 1210321 20180125
install 1210315 20180305