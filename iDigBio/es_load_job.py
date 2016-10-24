from __future__ import print_function
import os
import sys
import time
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types


sc = SparkContext(appName="iDigBioParquet")
sqlContext = SQLContext(sc)

dataset_date = time.strftime("%Y%m%d")
out_dir = "data/idigbio-{0}.parquet".format(dataset_date)

if os.path.isdir(out_dir):
    print("Output dir {0} exists".format(out_dir))
    exit

index_version = "2.10.2"
nodes = "c18node14.acis.ufl.edu,c18node2.acis.ufl.edu,c18node6.acis.ufl.edu,c18node10.acis.ufl.edu,c18node12.acis.ufl.edu"
query = '{"query": {"bool": {"must": [{"term":{"genus":"acer"}}]}}}'
fields = ("uuid,kingdom,phylum,order,class,family,genus,group,specificepithet,infraspecificepithet,scientificname,commonname,canonicalname,highertaxon,"
          "datecollected,continent,country,countrycode,stateprovince,municipality,waterbody,"
          "occurenceid,catalognumber,fieldnumber,collectioncode,collectionid,collector,barcodevalue,basisofrecord,individualcount"
)

(sqlContext.read.format("org.elasticsearch.spark.sql")
    .option("es.query", query)
    .option("es.read.field.include", fields)
    .option("es.nodes", nodes)
    .load("idigbio-{0}/records".format(index_version))
    .write.parquet(out_dir)
)
