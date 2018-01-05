

# getting the data

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

# manipulate in scala spark-shell

## open the data archive

Wiki data archive is a giant json array of items like:

```json
[
{ item1 },
{ item2 },
...
{ itemN }
]
```

with N >> 1M

Now, in spark shell, do something like:
```scala
val wikidata = spark.read.textFile("/guoda/data/source=wikidata/date=20171227/latest-all.json.bz2")

# select taxa chunks and turn into individual json objects
# taxa chunks are json object that mention taxon qualifier https://www.wikidata.org/wiki/Q16521
# turn into JSON Lines text format, also called newline-delimited JSON (see http://jsonlines.org/)
val taxaJsonString = wikidata.filter(_.contains("""Q16521"""")).map(_.stripLineEnd.replace(""",$""", ""))
```

For full example, see [wikidata/taxonlinks.scala](https://github.com/bio-guoda/guoda-datasets/blob/master/wikidata/taxonlinks.scala). You can copy-paste this example into spark-shell, or use ```:paste [filename]``` to run the commands. 



