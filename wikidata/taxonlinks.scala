// Apache Spark 2.2.1 script used to extract wikidata taxon
// concepts and associated taxonomic identifiers.
// Assumes a wikidata json dump to be available in hdfs (see below).
// for more information about wikidata dumps, see https://dumps.wikimedia.org/wikidatawiki/entities/ .


import java.time._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.SaveMode

val start = LocalDateTime.now()

import spark.implicits._

case class CommonName(language: String
                      , value: String)

case class TaxonTerm(id: String
                     , names: Seq[String]
                     , rankIds: Seq[String]
                     , parentIds: Seq[String]
                     , sameAsIds: Seq[String])

val taxonMap = Seq(("NCBI", "P685"),
  ("ITIS", "P815"),
  ("EOL", "P830"),
  ("FISHBASE", "P938"),
  ("GBIF", "P846"),
  ("IF", "P1391"),
  ("INAT_TAXON", "P41964"),
  ("WORMS", "P850"))

@transient
lazy implicit val formats = DefaultFormats

def isTaxonInstance(json: JValue) = (json \ "claims" \ "P31" \\ "mainsnak" \ "datavalue" \ "value" \ "numeric-id").extract[Option[Int]] match {
  case Some(16521) => true
  case _ => false
}

def taxonItemId(json: JValue) = {
  if (isTaxonInstance(json)) {
    (json \ "id").extract[Option[String]]
  } else None
}

def idMapForTaxon(json: JValue): Seq[(String, String)] = {
  taxonItemId(json) match {
    case Some(id) => {
      taxonMap.flatMap(entry => {
        (json \ "claims" \ entry._2 \\ "mainsnak" \ "datavalue" \ "value").extract[Option[String]] match {
          case Some(externalId) =>
            Some((s"${entry._1}:$externalId", s"$id"))
          case None => None
        }
      })
    }
    case None => Seq()
  }
}

def idsForTaxon(json: JValue): Seq[String] = {
  taxonItemId(json) match {
    case Some(id) => {
      taxonMap.flatMap(entry => {
        (json \ "claims" \ entry._2 \\ "mainsnak" \ "datavalue" \ "value").extract[Option[String]] match {
          case Some(externalId) =>
            Some(s"${entry._1}:$externalId")
          case None => None
        }
      })
    }
    case None => Seq()
  }
}

def extractList(selector: JValue): Seq[String] = {
  if (selector.isInstanceOf[JArray]) selector.extract[List[String]] else List(selector.extract[Option[String]]).flatten
}


def taxonItem(json: JValue) = {
  if (isTaxonInstance(json)) {
    val id = (json \ "id").extract[Option[String]]
    val rankIds = extractList(json \\ "P105" \ "mainsnak" \ "datavalue" \ "value" \ "id")
    val names = extractList(json \\ "P225" \ "mainsnak" \ "datavalue" \ "value")
    val parentIds = extractList(json \\ "P171" \ "mainsnak" \ "datavalue" \ "value" \ "id")

    Some(TaxonTerm(id = id.getOrElse("")
      , names = names
      , rankIds = rankIds
      , parentIds = parentIds
      , sameAsIds = idsForTaxon(json)))
  } else None
}

// this assumes that wikidata was imported using importWikidata.sh or similar.

val wikidata = spark.read.textFile("/guoda/data/source=wikidata/date=20171227/latest-all.json.bz2")

// select taxa chunks and turn into individual json objects
//
// taxa chunks are json object that mention taxon qualifier https://www.wikidata.org/wiki/Q16521
// turn into JSON Lines text format, also called newline-delimited JSON (see http://jsonlines.org/)
val taxaJsonString = wikidata.filter(_.contains("""Q16521"""")).map(_.stripLineEnd.replaceFirst(""",$""", ""))

import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._

// extracts taxon links in form Seq((wikidata item, taxon id))
// for example (Q140, GBIF:5219404)
val taxonLinksParsed = taxaJsonString.flatMap(line => idMapForTaxon(parse(line)))

// show first 10 links
taxonLinksParsed.take(10)

// write all to parquet file for fast subsequent processing
taxonLinksParsed.write.mode(SaveMode.Overwrite).parquet(s"/guoda/data/source=wikidata/date=20171227/taxonLinks.parquet")

val taxonLinks = spark.read.parquet("/guoda/data/source=wikidata/date=20171227/taxonLinks.parquet")
val taxonLinksDS = taxonLinks.as[(String, String)]

//taxonInfoParsed.write.mode(SaveMode.Overwrite).parquet("/guoda/data/source=wikidata/date=20171227/taxonInfo.parquet")

// https://www.wikidata.org/wiki/Q2405884 has multiple names
val taxonInfo = spark.read.parquet("/guoda/data/source=wikidata/date=20171227/taxonInfo.parquet")

taxonInfo.count

val duplicateNames = taxonInfo.as[TaxonTerm].map(x => (x.names.length, x.id, x.names.mkString("|"))).filter(_._1 > 1)

duplicateNames.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save("/guoda/data/source=wikidata/date=20171227/duplicateNames.csv")

val duplicateRanks = taxonInfo.as[TaxonTerm].map(x => (x.rankIds.length, x.id, x.rankIds.mkString("|"))).filter(_._1 > 1)

duplicateRanks.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save("/guoda/data/source=wikidata/date=20171227/duplicateRanks.csv")

val duplicateParentIds = taxonInfo.as[TaxonTerm].map(x => (x.parentIds.length, x.id, x.parentIds.mkString("|"))).filter(_._1 > 1)

duplicateParentIds.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save("/guoda/data/source=wikidata/date=20171227/duplicateParentIds.csv")

val sameAsIdStats = taxonInfo.as[TaxonTerm].map(x => (x.sameAsIds.length, 1)).rdd.reduceByKey(_ + _).toDS

sameAsIdStats.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save("/guoda/data/source=wikidata/date=20171227/sameAsIdsStats.csv")

// this assumes that you have some GloBI taxon cache loaded using importGloBI.sh or similar.

val globiTaxonMap = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load("/guoda/data/source=globi/date=20180220/taxonMap.tsv.bz2")
val globiTaxonCache = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load("/guoda/data/source=globi/date=20180220/taxonCache.tsv.bz2")

case class TaxonMap(providedTaxonId: String = "", providedTaxonName: String = "", resolvedTaxonId: String = "", resolvedTaxonName: String = "")
case class TaxonCache(id: String, name: String, rank: String, commonNames: String, path: String, pathIds: String, pathNames: String, externalUrl: String, thumbnailUrl: String)

val taxonMapGloBI = globiTaxonMap.as[TaxonMap] 
val taxonCacheGloBI = globiTaxonCache.as[TaxonCache]

val wikidataInfo = taxonInfo.as[TaxonTerm]
val wikidataInfoNotEmpty = wikidataInfo.filter(_.names.nonEmpty).filter(_.rankIds.nonEmpty)

val taxonMapWikidata = wikidataInfoNotEmpty.flatMap(row => row.sameAsIds.map(id => TaxonMap(s"WD:${row.id}", row.names.head, id, "")))

val taxonCacheWikidata = wikidataInfoNotEmpty.map(row => TaxonCache(id = s"WD:${row.id}", name = row.names.head, rank = s"WD:${row.rankIds.head}", commonNames = "", path = (row.parentIds.map(id => "") ++ Seq(row.names.head)).mkString("|"), pathIds = (row.parentIds.map(id => s"WD:$id") ++ Seq(s"WD:${row.id}")).mkString("|"), pathNames = (row.parentIds.map(id => "") ++ Seq(s"WD:${row.rankIds.head}")).mkString("|"), externalUrl=s"https://www.wikidata.org/wiki/${row.id}", thumbnailUrl=""))

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


val taxonMapCombined = mappedMap.map(_._2).filter(!hasWikiData(_)).distinct
taxonMapCombined.as[TaxonMap].coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter", "\t").save("/guoda/data/source=globi/date=20180404/taxonMap.tsv")
taxonCacheCombined.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter", "\t").save("/guoda/data/source=globi/date=20180404/taxonCache.tsv")

val taxonMapCombinedLoad = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").load("/guoda/data/source=globi/date=20180404/taxonMap.tsv")

val end = LocalDateTime.now()

val durationInSeconds = java.time.Duration.between(start, end).getSeconds

println(s"merging Wikidata taxa into GloBI Taxon Graph took ${durationInSeconds} and started at ${start}")
