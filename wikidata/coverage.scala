import org.json4s._
import org.json4s.jackson.JsonMethods._


@transient
lazy implicit val formats = DefaultFormats


import spark.implicits._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._


//val wdTaxonLinks = spark.read.parquet("/guoda/data/source=wikidata/date=20171227/taxonLinks.parquet")
//val taxonLinksDS = taxonLinks.as[(String, String)]

// data experiments using GloBI taxon v0.4.2, ott 3.0, and wd 20171227 .

val wd = spark.read.format("csv").option("delimiter", "\t").option("header", "true").load("/home/jorrit/proj/wikidata/data/wikiDataTaxonLinks20171227.tsv.gz")
val wkLinks = wd.as[(String, String)].map(link => (link._2, s"WD:${link._1}"))

val ott = spark.read.format("csv").option("delimiter", "|").option("header", "true").load("/home/jorrit/proj/wikidata/ott/taxonomy.tsv")
val ottLinks = ott.as[(String, String, String, String, String, String, String, String)].flatMap(row => row._5.toUpperCase.split(",").map(id => (id.trim, s"OTT:${row._1}".trim)))

val globi = spark.read.format("csv").option("delimiter", "\t").option("header", "true").load("/home/jorrit/proj/wikidata/data/taxon-0.4.2/taxonMap.tsv.gz")
val globiLinks = globi.as[(String, String, String, String)].map(row => (row._3, s"GLOBI:${row._1}@${row._2}")).filter(link => link._1 != null && !link._1.startsWith("OTT:") && !link._1.startsWith("WD:"))

// combine the link tables
// something -> {ott|wd|globi}
val links = ottLinks.union(wkLinks).union(globiLinks)

// links all external ids to respective, wd, ott, and globi terms
val groupedBy = links.map(link => (link._1, Seq(link._2))).rdd.reduceByKey(_ ++ _)

val taxonSchemes = List("NCBI","IF","GBIF", "WORMS", "IF")

val groupedById = groupedBy.filter(x => taxonSchemes.contains(x._1.split(":").head)).cache()

// all unique external ids used
groupedById.map(_._1).distinct.count
// res35: Long = 4787719

// all three ott,wd,globi have mappings to an id
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct.length)).filter(_._2 == 3).count
//res36: Long = 207958

// all ids with no globi matches
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct.filter(_ != "GLOBI").length)).filter(_._2 == 2).count
//res37: Long = 2215952

// all ids with ott ids
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct.filter(_ == "OTT").length)).filter(_._2 == 1).count
//res38: Long = 4410963

// all ids with wd ids
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct.filter(_ == "WD").length)).filter(_._2 == 1).count
//res39: Long = 2554425

// only ids with globi ids
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct.filter(_ == "GLOBI").length)).filter(_._2 == 1).count
//res40: Long = 332182

// only ids with only a globi id
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 1).flatMap(_._2).filter(_ == "GLOBI").count
// res41: Long = 38283

// ids that exclusively link to ott
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 1).flatMap(_._2).filter(_ == "OTT").count
// res42: Long = 2117549

// ids that exclusively link to wd
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 1).flatMap(_._2).filter(_ == "WD").count
// res43: Long = 329994


// some 2 non-globi terms
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 2).filter(!_._2.contains("GLOBI")).distinct.count
// res44: Long = 2007994

// some 2 non-ott terms
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 2).filter(!_._2.contains("OTT")).distinct.count
// res0: Long = 8479

// some 2 non-wd terms
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 2).filter(!_._2.contains("WD")).distinct.count
// res1: Long = 77462

// any non-globi terms
groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(!_._2.contains("GLOBI")).distinct.count
//res45: Long = 4455537

// globi only matches by naming scheme
val breakDownOfGloBIOnlyLinks = groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 1).filter(_._2.contains("GLOBI")).map(r => (r._1.split(":").head, 1)).reduceByKey(_ + _).take(10)
// breakDownOfGloBIOnlyLinks: Array[(String, Int)] = Array((WORMS,2061), (NCBI,12652), (GBIF,21309), (IF,2261))

val breakDownOfOTTOnlyLinks = groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 1).filter(_._2.contains("OTT")).map(r => (r._1.split(":").head, 1)).reduceByKey(_ + _).take(10)
// breakDownOfOTTOnlyLinks: Array[(String, Int)] = Array((WORMS,97172), (NCBI,919617), (GBIF,896879), (IF,203881))

val breakDownOfWDOnlyLinks = groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 1).filter(_._2.contains("WD")).map(r => (r._1.split(":").head, 1)).reduceByKey(_ + _).take(10)
// breakDownOfWDOnlyLinks: Array[(String, Int)] = Array((WORMS,59464), (NCBI,19779), (GBIF,243646), (IF,7105))

val breakDownOfOverlappingLinks = groupedById.map(row => (row._1, row._2.map(_.split(":").head).distinct)).filter(_._2.length == 3).map(r => (r._1.split(":").head, 1)).reduceByKey(_ + _).take(10)
// breakDownOfOverlappingLinks: Array[(String, Int)] = Array((WORMS,22637), (NCBI,71980), (GBIF,103317), (IF,10024))

val breakDownOfAllLinks = groupedById.map(r => (r._1.split(":").head, 1)).reduceByKey(_ + _).take(10)


// id1 --> ott/wd/globi --> globi --> id1,id2,id3
//                 | L----> ott   --> id1,id5,id6
//                 L------> wd    --> id1,id5,id6
//
// use id1 as a grouping of ott/wd/globi so that all can be expanded to do consistency check
// across taxonomies.

// id -> (globi|ott|wd) combo
val idToComposites = groupedById.cache().map(r => (r._1, r._2.partition(_.startsWith("GLOBI")))).filter(_._2._1.nonEmpty).flatMap(r => r._2._1.map(g => (r._1, (r._2._2 ++ Seq(g)).sorted.mkString("|"))))

// (globi|ott|wd) -> (id1, id2, id3)
//      where id1, id2, id3 are equivalent ids from globi,ott,wd
val reducedByComposites = idToComposites.map(r => (r._2, Seq(r._1))).reduceByKey(_ ++ _).map(r => (r._1, r._2.distinct))

// inspect examples where ids are mapped to multiple ids from similar naming scheme

val compositesWithMultipleMappingForSinglePrefix = groupedByComposites.map(r => { val prefixes = r._2.distinct.map(_.split(":").head); val reps = prefixes.length - prefixes.distinct.length; val keyLength = r._1.split("""\|""").length; (r._1, r._2, reps, keyLength) })

compositesWithMultipleMappingForSinglePrefix.take(5)

// calculate number of possibly inconsistent mappings (multiple id matches with a scheme) by size of composite key
compositesWithMultipleMappingForSinglePrefix.map(r => (r._4, r._3)).reduceByKey((agg, v) => agg + (if (v > 0) 1 else 0)).take(5)
// the more overlap, the fewer duplicates in mappings
// res38: Array[(Int, Int)] = Array((1,1510), (2,14), (3,0), (4,0))

compositesWithMultipleMappingForSinglePrefix.map(r => (r._4, r._3)).filter(_._2 > 0).map(r => (r._1, 1)).reduceByKey(_ + _).take(5)
// res58: Array[(Int, Int)] = Array((1,8975), (2,56))

// write duplicates with pair composite (e.g., globi + {ott|wd})
 compositesWithMultipleMappingForSinglePrefix.filter(r => r._3 > 0 && r._4 == 2).map(r => (r._1, r._2.mkString("|"), r._3, r._4)).toDF.coalesce(1).write.option("delimiter", "\t").format("csv").save("/home/jorrit/wikidata/duplicatesWithCompositeKeyPair20180316.tsv")

// write multiples with singleton composite (e.g., only globi name)
compositesWithMultipleMappingForSinglePrefix.filter(r => r._3 > 0 && r._4 == 1).map(r => (r._1, r._2.mkString("|"), r._3, r._4)).toDF.coalesce(1).write.option("delimiter", "\t").format("csv").save("/home/jorrit/wikidata/duplicatesWithCompositeKeySingleton20180316.tsv")
