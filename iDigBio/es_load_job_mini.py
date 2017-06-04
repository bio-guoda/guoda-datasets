# A quick test job to exercise the whole stack w/o rewriting this to be more
# general.

from __future__ import print_function
import os
import sys
import time
import requests
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types

sc = SparkContext(appName="iDigBioParquetMini")
sqlContext = SQLContext(sc)

out_dir = "/tmp"
out_fn_base = "idigbio-minitest"
dataset_date = time.strftime("%Y%m%d%S")
nodes = "c18node14.acis.ufl.edu,c18node2.acis.ufl.edu,c18node6.acis.ufl.edu,c18node10.acis.ufl.edu,c18node12.acis.ufl.edu"
index = "idigbio"
query = '{"query": {"bool": {"must": [{"term":{"genus":"eucalyptus"}}]}}}'

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

# Read in dataframe
df = (sqlContext.read.format("org.elasticsearch.spark.sql")
    .option("es.read.field.include", fields)
    .option("es.nodes", nodes)
    .option("es.query", query)
    .load("{0}/records".format(index))
    .cache()
)

# Write out the whole thing
(df
    .write
    .mode("overwrite")
    .parquet(os.path.join(out_dir,
                          "{0}-{1}.parquet".format(out_fn_base, dataset_date)))
)

# Write out a small 100k version for testing
(df
    .limit(100 * 1000)
    .write
    .mode("overwrite")
    .parquet(os.path.join(out_dir,
                          "{0}-{1}-100k.parquet".format(out_fn_base, dataset_date)))
)

# Write out a larger 1M version for testing
(df
    .limit(1000 * 1000)
    .write
    .mode("overwrite")
    .parquet(os.path.join(out_dir,
                          "{0}-{1}-1M.parquet".format(out_fn_base, dataset_date)))
)
