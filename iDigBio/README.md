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


### References

[ntegrating Hadoop and Elasticsearch – Part 2](https://db-blog.web.cern.ch/blog/prasanth-kothuri/2016-05-integrating-hadoop-and-elasticsearch-%E2%80%93-part-2-%E2%80%93-writing-and-querying)
