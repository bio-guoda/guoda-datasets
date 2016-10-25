# iDigBio Data Loaded From ES Indexes

This code will load the iDigBio data from Elastic Search indexes in the 
production cluster and write out a dataframe with it represented nicely.


## The Elastic Search Connector

Connections to Elastic Search are made using the elastic.co Java .jar module. 
They are on version 2.4 and about to release 5.0 (Sun-esq version jump). The
word "hadoop" is used because the API the plugin uses is the one for Hadoop,
there is no dependancy on the Hadoop ecosystem to use this with Spark.

The source is located here:

[https://github.com/elastic/elasticsearch-hadoop](https://github.com/elastic/elasticsearch-hadoop)

Compiled versions are here:

[https://www.elastic.co/downloads/hadoop](https://www.elastic.co/downloads/hadoop)

And the full documentation which only had a brief example at the bottom for Python
is here:

[https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html)

### Installation

1. Download from https://www.elastic.co/downloads/hadoop
1. Copy the right .jar into the Spark jars/ directory where the file should be
`elasticsearch-spark-<Spark version>-<ES plugin version>.jar`. You can find the
Spark version that came with Spark by starting `spark-shell`.
1. Start pyspark with `--conf spark.es.nodes="c18node14.acis.ufl.edu"` on the 
command line. See blog post below for more options.


### Running

Probably only needs about 10G of memory (data size is 6.7G with the limited fields at the moment):

time spark-submit --driver-memory 50g es_load_job.py

Example on Okapi:

real    44m29.128s
user    102m56.928s
sys     6m44.064s



### References

[ntegrating Hadoop and Elasticsearch – Part 2](https://db-blog.web.cern.ch/blog/prasanth-kothuri/2016-05-integrating-hadoop-and-elasticsearch-%E2%80%93-part-2-%E2%80%93-writing-and-querying)


pyspark --driver-class-path=elasticsearch-spark-20_2.11-5.0.0-beta1.jar --conf spark.es.nodes="c18node14.acis.ufl.edu,c18node2.acis.ufl.edu,c18node6.acis.ufl.edu,c18node10.acis.ufl.edu,c18node12.acis.ufl.edu" 


https://www.google.com/search?q=typically+this+occurs+with+arrays+which+are+not+mapped+as+single+value&oq=typically+this+occurs+with+arrays+which+are+not+mapped+as+single+value&aqs=chrome..69i57.1196j0j7&sourceid=chrome&ie=UTF-8


Have issues with array fields getting detected in schema:

16/10/10 09:54:48 WARN ScalaRowValueReader: Field 'flags' is backed by an array but the associated Spark Schema does not reflect this;
              (use es.read.field.as.array.include/exclude) 
16/10/10 09:54:48 WARN ScalaRowValueReader: Field 'indexData.dwc:multimedia' is backed by an array but the associated Spark Schema does not reflect this;
              (use es.read.field.as.array.include/exclude) 


Have an error with this field:

org.elasticsearch.hadoop.EsHadoopIllegalStateException: Field 'indexData.dwc:multimedia.dcterms:source' not found; typically this occurs with arrays which are not mapped as single value

indexData.dwc:multimedia is an array in ES as is indexData.dwc, nested arrays

Discussion of data mapping, see "Handling array/multi-value fields"

https://www.elastic.co/guide/en/elasticsearch/hadoop/current/mapping.html

There doesn't seem to be anything special about this field, arrays detected as structs that are nullable according to printSchema().

https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html

It looks like we need to help out the mapper, es.field.read.as.array.include. How and where to set this?

https://spark.apache.org/docs/1.5.0/api/python/_modules/pyspark/conf.html
http://stackoverflow.com/questions/32362783/how-to-change-sparkcontext-properties-in-interactive-pyspark-session

Looks like making a conf object before creating the context. Can we really not change this at runtime? 
"
once a SparkConf object is passed to Spark, it is cloned and can no longer be modified by the user. Spark does not support modifying the configuration at runtime. 
"

Guess so...
