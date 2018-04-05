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

import spark.implicits._

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
val baseDir = "/guoda/data/source=wikidata/date=20171227/"
val wikidata = spark.read.textFile(s"$baseDir/latest-all.json.bz2")

// select taxa chunks and turn into individual json objects
//
// taxa chunks are json object that mention taxon qualifier https://www.wikidata.org/wiki/Q16521
// turn into JSON Lines text format, also called newline-delimited JSON (see http://jsonlines.org/)
val taxaJsonString = wikidata.filter(_.contains("""Q16521"""")).map(_.stripLineEnd.replaceFirst(""",$""", ""))

val taxonInfoParsed = taxaJsonString.flatMap(line => taxonItem(parse(line)))
taxonInfoParsed.write.mode(SaveMode.Overwrite).parquet(s"$baseDir/taxonInfo.parquet")
// https://www.wikidata.org/wiki/Q2405884 has multiple names
val taxonInfo = spark.read.parquet(s"$baseDir/taxonInfo.parquet")


// extracts taxon links in form Seq((wikidata item, taxon id))
// for example (Q140, GBIF:5219404)
val taxonLinksParsed = taxonInfo.as[TaxonTerm].flatMap(term => term.sameAsIds.map(id => (term.id, id)))

// write all to parquet file for fast subsequent processing
taxonLinksParsed.write.mode(SaveMode.Overwrite).parquet(s"$baseDir/taxonLinks.parquet")

val taxonLinks = spark.read.parquet(s"$baseDir/taxonLinks.parquet")
val taxonLinksDS = taxonLinks.as[(String, String)]


val duplicateNames = taxonInfo.as[TaxonTerm].map(x => (x.names.length, x.id, x.names.mkString("|"))).filter(_._1 > 1)

duplicateNames.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save(s"$baseDir/duplicateNames.csv")

val duplicateRanks = taxonInfo.as[TaxonTerm].map(x => (x.rankIds.length, x.id, x.rankIds.mkString("|"))).filter(_._1 > 1)

duplicateRanks.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save(s"$baseDir/duplicateRanks.csv")

val duplicateParentIds = taxonInfo.as[TaxonTerm].map(x => (x.parentIds.length, x.id, x.parentIds.mkString("|"))).filter(_._1 > 1)

duplicateParentIds.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save(s"$baseDir/duplicateParentIds.csv")

val sameAsIdStats = taxonInfo.as[TaxonTerm].map(x => (x.sameAsIds.length, 1)).rdd.reduceByKey(_ + _).toDS

sameAsIdStats.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").save(s"$baseDir/sameAsIdsStats.csv")

val end = LocalDateTime.now()

val durationInSeconds = java.time.Duration.between(start, end).getSeconds

println(s"extracting taxa from Wikidata dump took [$durationInSeconds]s and started at [$start]")
