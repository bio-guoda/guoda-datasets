This page contains some experiments on how to get all of wikidata and mine it using scala/spark. 

Before attempting to get and import the data, please check whether a version already exists in the guoda hdfs cluster at hdfs://guoda/data/source=wikidata .

# get data

```
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
```

Note that it is important to grab the .bz2 archive to allow for parallel ingestion of data. Archives with .gz have to be imported sequentially. 

# import data

in terminal, import dataset into hdfs -
```
hdfs dfs -put latest-all.json.bz2 hdfs://guoda/data/source=wikidata/date=20171227/latest.all.json.bz2
```

where 20171227 is the date of the wikidata dump. 

# mine data

## extract json objects

Wiki data archive is a giant json array of items like 

```
[
{ item1 },
{ item2 },
...
{ itemN }
]
```

with N >> 1M

For full examples, see [lion.json](./lion.json) and [cat](./cat.json).

Now, in spark shell, do something like:
```scala
val wikidata = spark.read.textFile("/guoda/data/source=wikidata/date=20171227/latest-all.json.bz2")

// select taxa chunks and turn into individual json objects
// taxa chunks are json object that mention taxon qualifier https://www.wikidata.org/wiki/Q16521
// turn into JSON Lines text format, also called newline-delimited JSON (see http://jsonlines.org/)
val taxaJsonString = wikidata.filter(_.contains("""Q16521"""")).map(_.stripLineEnd.replace(""",$""", ""))
```

## extract taxon links

Wikidata contains taxon items (e.g., [lion](https://www.wikidata.org/wiki/Q140))with rich associations to all sorts of data including taxonomies, images, commonnames and more. The example below shows how to select taxon items and extract taxon ids for various taxonomic systems (e.g., GBIF, ITIS, NCBI, EOL, WoRMS). 

For full example, see [wikidata/taxonlinks.scala](https://github.com/bio-guoda/guoda-datasets/blob/master/wikidata/taxonlinks.scala). You can copy-paste this example into spark-shell, or use ```:paste [filename]``` to run the commands.

An example run in a spark shell looks like:

```
Spark session available as 'spark'.
Welcome to                                                                                                                         
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.0
      /_/

Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_151)
Type in expressions to have them evaluated.
Type :help for more information.

scala> :paste "taxonlinks.scala"
Pasting file taxonlinks.scala...
import org.json4s._
import org.json4s.jackson.JsonMethods._
formats: org.json4s.DefaultFormats.type = <lazy>
isTaxonInstance: (json: org.json4s.JValue)Boolean
taxonItemId: (json: org.json4s.JValue)Option[String]
idMapForTaxon: (json: org.json4s.JValue)Seq[(String, String)]
wikidata: org.apache.spark.sql.Dataset[String] = [value: string]
taxaJsonString: org.apache.spark.sql.Dataset[String] = [value: string]
import spark.implicits._
taxonLinks: org.apache.spark.sql.Dataset[(String, String)] = [_1: string, _2: string]
res0: Array[(String, String)] = Array((Q140,NCBI:9689), (Q140,ITIS:183803), (Q140,EOL:328672), (Q140,GBIF:5219404), (Q606,NCBI:1962
58), (Q606,ITIS:563933), (Q606,EOL:794643), (Q606,GBIF:2450372), (Q787,NCBI:9825), (Q787,ITIS:898917))
```

## count unique taxon items 

After saving the taxon links in a parquet format, calculations can get quite fast.

For instance, counting the number of unique wikidata taxon items with at least one external taxonomic identifier take less than a minute:

```scala

val links = spark.read.parquet("/guoda/data/source=wikidata/date=20171227/taxonlinks.parquet") 
links.as[(String, String)].map(link => link._1).distinct.count 
// res4: Long = 1953706                                                     
// ~ about 2M taxon wikidata items

links.as[(String, String)].map(link => link._2).filter(_.startsWith("EOL")).distinct.count 
// res5: Long = 1351475
// ~ 1.3M eol unique taxon ids

```

