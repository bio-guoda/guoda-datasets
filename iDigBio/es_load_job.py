from __future__ import print_function
import os
import sys
import time
import requests
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types


sc = SparkContext(appName="iDigBioParquet")
sqlContext = SQLContext(sc)

dataset_date = time.strftime("%Y%m%d")
out_dir = "/guoda/data/idigbio-{0}.parquet".format(dataset_date)

# This doesn't work for checking HDFS file system, will always overwrite output
if os.path.isdir(out_dir):
    print("Output dir {0} exists, use --overwrite?".format(out_dir))
    exit

index = "idigbio"
nodes = "c18node14.acis.ufl.edu,c18node2.acis.ufl.edu,c18node6.acis.ufl.edu,c18node10.acis.ufl.edu,c18node12.acis.ufl.edu"
#query = '{"query": {"bool": {"must": [{"term":{"stateprovince":"florida"}}]}}}'

# Get field list from API endpoint
meta_fields_records = (requests
                       .get("http://search.idigbio.org/v2/meta/fields/records")
                       .json()
                       )
field_set = set()
for k,v in meta_fields_records.items():
    if v.get("fieldName", False):
        field_set.add(k)
    if k == "data":
        for kd,vd in v.items():
            if vd.get("fieldName", False):
                field_set.add("data.{0}".format(kd))

# Remove known fields that cause problems
bad_field_set = set({'commonnames', 'flags', 'recordids', 'mediarecords'})
field_set -= bad_field_set
fields = ",".join(field_set)

# Write out dataframe
#    .option("es.query", query)
(sqlContext.read.format("org.elasticsearch.spark.sql")
    .option("es.read.field.include", fields)
    .option("es.nodes", nodes)
    .load("{0}/records".format(index))
    .write
    .mode("overwrite")
    .parquet(out_dir)
)
