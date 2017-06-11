# Read iDigBio from Elastic Search and write it to a parquet
from __future__ import print_function
import os
import sys
import time
import requests
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types

sc = SparkContext(appName="iDigBioMediaParquet")
sqlContext = SQLContext(sc)

out_dir = "/guoda/data"
out_fn_base = "idigbio-media"
dataset_date = time.strftime("%Y%m%dT%H%M%S")
nodes = "c18node14.acis.ufl.edu,c18node2.acis.ufl.edu,c18node6.acis.ufl.edu,c18node10.acis.ufl.edu,c18node12.acis.ufl.edu"
index = "idigbio"
#query = """{"query": {
#    "bool": {
#        "must": [
#            {
#                "term": {"recordset": "a2b36fdf-50bc-44ef-a6a4-ca6dc1dc148a"}
#            }
#        ]
#}}}"""

# Get field list from API endpoint
meta_fields_records = (requests
                       .get("http://search.idigbio.org/v2/meta/fields/mediarecords")
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
bad_field_set = set(["flags", "recordids", "records"])
field_set -= bad_field_set

# Code to help binary search for a field that's not working, comment out when running live
#field_set = sorted(field_set)
#field_set = field_set[0:10]
#field_set = ["uuid"]
#field_set += ["uuid"]
#print("#"*80)
#print("\n".join(field_set))
#print("#"*80)
#sys.exit()


fields = ",".join(field_set)

#fields = "uuid,licenselogourl"

# Read in dataframe
#https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
#es.input.max.docs.per.partition (default 100000) 
#    .option("es.query", query)
df = (sqlContext.read.format("org.elasticsearch.spark.sql")
    .option("es.read.field.include", fields)
    .option("es.nodes", nodes)
    .load("{0}/mediarecords".format(index))
)

# Write out the whole thing
(df
    .write
    .parquet(os.path.join(out_dir,
                          "{0}-{1}.parquet".format(out_fn_base, dataset_date)))
)

# Write out a small 100k version for testing
(df
    .limit(100 * 1000)
    .write
    .parquet(os.path.join(out_dir,
                          "{0}-{1}-100k.parquet".format(out_fn_base, dataset_date)))
)

# Write out a larger 1M version for testing
(df
    .limit(1000 * 1000)
    .write
    .parquet(os.path.join(out_dir,
                          "{0}-{1}-1M.parquet".format(out_fn_base, dataset_date)))
)
