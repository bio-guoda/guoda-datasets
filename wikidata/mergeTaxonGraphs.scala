// Apache Spark 2.2.1 script used to extract wikidata taxon
// concepts and associated taxonomic identifiers.
// Assumes a wikidata json dump to be available in hdfs (see below).
// for more information about wikidata dumps, see https://dumps.wikimedia.org/wikidatawiki/entities/ .


import java.time._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._

val start = LocalDateTime.now()

case class CommonName(language: String
                      , value: String)

case class TaxonTerm(id: String = ""
                     , names: Seq[String] = Seq()
                     , rankIds: Seq[String] = Seq()
                     , parentIds: Seq[String] = Seq()
                     , sameAsIds: Seq[String] = Seq())

case class TaxonMap(providedTaxonId: String = "", providedTaxonName: String = "", resolvedTaxonId: String = "", resolvedTaxonName: String = "")
case class TaxonCache(id: String, name: String, rank: String, commonNames: String, path: String, pathIds: String, pathNames: String, externalUrl: String)

import spark.implicits._

val baseDir = "/guoda/data/"
val baseDirWikidata = baseDir + "/source=wikidata/date=20171227"
val baseDirGloBI = baseDir + "/source=globi/date=20180305"

val taxonInfo = spark.read.parquet(s"$baseDirWikidata/taxonInfo.parquet")

// this assumes that you have some GloBI taxon cache loaded using installGloBITaxonGraph.sh or similar.

val globiTaxonMap = spark.read.format("csv").option("delimiter", "\t").option("header", "true").load(s"$baseDirGloBI/taxonMap.tsv.bz2")
val globiTaxonCache = spark.read.format("csv").option("delimiter", "\t").option("header", "true").load(s"$baseDirGloBI/taxonCache.tsv.bz2")


val taxonMapGloBI = globiTaxonMap.as[TaxonMap] 
val taxonCacheGloBI = globiTaxonCache.as[TaxonCache]

val wikidataInfo = taxonInfo.as[TaxonTerm]
val wikidataInfoNotEmpty = wikidataInfo.filter(_.names.nonEmpty).filter(_.sameAsIds.nonEmpty).filter(_.rankIds.nonEmpty)

val taxonMapWikidata = wikidataInfoNotEmpty.flatMap(row => row.sameAsIds.map(id => TaxonMap(s"WD:${row.id}", row.names.head, id, "")))

val taxonCacheWikidata = wikidataInfoNotEmpty.map(row => TaxonCache(id = s"WD:${row.id}", name = row.names.head, rank = s"WD:${row.rankIds.head}", commonNames = "", path = (row.parentIds.map(id => "") ++ Seq(row.names.head)).mkString("|"), pathIds = (row.parentIds.map(id => s"WD:$id") ++ Seq(s"WD:${row.id}")).mkString("|"), pathNames = (row.parentIds.map(id => "") ++ Seq(s"WD:${row.rankIds.head}")).mkString("|"), externalUrl=s"https://www.wikidata.org/wiki/${row.id}"))

// combine the caches
val taxonCacheCombined = taxonCacheGloBI.union(taxonCacheWikidata)

// infer wikidata links from shared external ids
val mapByKey = taxonMapGloBI.map(x => (x.resolvedTaxonId, x)).union(taxonMapWikidata.map(x => (x.resolvedTaxonId, x)))

def hasWikidata(x: TaxonMap) = {
  x.providedTaxonId != null && x.providedTaxonId.startsWith("WD:")
}

def swapWikidata(x: TaxonMap, y: TaxonMap) = {
  if (hasWikidata(x)) {
    TaxonMap(y.providedTaxonId, y.providedTaxonName, x.providedTaxonId, x.providedTaxonName)
  } else if (hasWikidata(y)) {
    TaxonMap(x.providedTaxonId, x.providedTaxonName, y.providedTaxonId, y.providedTaxonName)
  } else {
    x
  }
}

val mappedMap = mapByKey.rdd.reduceByKey( (x,y) => swapWikidata(x,y) )

val taxonMapCombined = mappedMap.map(_._2).filter(!hasWikidata(_)).distinct
taxonMapCombined.toDS.distinct.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter", "\t").save("/guoda/data/source=globi/date=20180404/taxonMap.tsv")
taxonCacheCombined.distinct.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter", "\t").save("/guoda/data/source=globi/date=20180404/taxonCache.tsv")

val taxonMapCombinedLoad = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load("/guoda/data/source=globi/date=20180404/taxonMap.tsv")

val end = LocalDateTime.now()

val durationInSeconds = java.time.Duration.between(start, end).getSeconds

println(s"merging Wikidata Taxon Graph into GloBI Taxon Graph took [$durationInSeconds]s and started at [$start]")
