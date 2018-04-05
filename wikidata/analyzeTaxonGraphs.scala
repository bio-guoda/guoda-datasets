// Apache Spark 2.2.x script used to calculate coverage statistics

import spark.implicits._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._

// assuming GloBI, OTT and Wikidata taxon graphs have been installed using install scripts
var datadir = "/guoda/data/"

// load wikidata links extracted via taxonlinks.scala script
val wd = spark.read.parquet(datadir + "/source=wikidata/date=20171227/taxonLinks.parquet")
val wkLinks = wd.as[(String, String)].map(link => (link._2, s"WD:${link._1}"))

// load Open Tree of Life Taxonomy
val ott = spark.read.format("csv").option("delimiter", "|").option("header", "true").load(datadir + "/source=ott/date=20170226/taxonomy.tsv")
val ottLinks = ott.as[(String, String, String, String, String, String, String, String)].flatMap(row => row._5.toUpperCase.split(",").map(id => (id.trim, s"OTT:${row._1}".trim)))

// load GloBI Taxon Cache v0.4.2
val globi = spark.read.format("csv").option("delimiter", "\t").option("header", "true").load(datadir + "/source=globi/date=20180305/taxonMap.tsv.bz2")
val globiLinks = globi.as[(String, String, String, String)].map(row => (row._3, s"GLOBI:${row._1}@${row._2}")).filter(link => link._1 != null && !link._1.startsWith("OTT:") && !link._1.startsWith("WD:"))

// combine the link tables
// something -> {ott|wd|globi}
val links = ottLinks.union(wkLinks).union(globiLinks).cache()

// links all external ids to respective, wd, ott, and globi terms
val groupedBy = links.map(link => (link._1, Seq(link._2))).rdd.reduceByKey(_ ++ _)

val taxonSchemes = List("NCBI","IF","GBIF", "WORMS", "IF")

val groupedById = groupedBy.filter(x => taxonSchemes.contains(x._1.split(":").head)).cache()

// links stats for OTT
links.map(x => (x._1.split(":").head, x._2.split(":").head)).filter(x => taxonSchemes.contains(x._1)).filter(_._2 == "OTT").map(x => (x._1, 1)).rdd.reduceByKey(_ + _).take(10)
// res0: Array[(String, Int)] = Array((WORMS,327929), (NCBI,1355207), (GBIF,2451566), (IF,276262))

// links stats for WD
links.map(x => (x._1.split(":").head, x._2.split(":").head)).filter(x => taxonSchemes.contains(x._1)).filter(_._2 == "WD").map(x => (x._1, 1)).rdd.reduceByKey(_ + _).take(10)
// res1: Array[(String, Int)] = Array((WORMS,288110), (NCBI,410092), (GBIF,1779789), (IF,76497))

// links stats for GloBI
links.map(x => (x._1.split(":").head, x._2.split(":").head)).filter(x => taxonSchemes.contains(x._1)).filter(_._2 == "GLOBI").map(x => (x._1, 1)).rdd.reduceByKey(_ + _).take(10)
// res2: Array[(String, Int)] = Array((WORMS,68565), (NCBI,704361), (GBIF,315173), (IF,33400))

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

// the more overlap, the fewer duplicates in mappings
compositesWithMultipleMappingForSinglePrefix.map(r => (r._4, r._3)).filter(_._2 > 0).map(r => (r._1, 1)).reduceByKey(_ + _).take(5)
// res58: Array[(Int, Int)] = Array((1,8975), (2,56))

// write duplicates with pair composite (e.g., globi + {ott|wd})
compositesWithMultipleMappingForSinglePrefix.filter(r => r._3 > 0 && r._4 == 2).map(r => (r._1, r._2.mkString("|"), r._3, r._4)).toDF.coalesce(1).write.option("delimiter", "\t").format("csv").save(datadir + "/duplicatesWithCompositeKeyPair20180316.tsv")

// write multiples with singleton composite (e.g., only globi name)
compositesWithMultipleMappingForSinglePrefix.filter(r => r._3 > 0 && r._4 == 1).map(r => (r._1, r._2.mkString("|"), r._3, r._4)).toDF.coalesce(1).write.option("delimiter", "\t").format("csv").save(datadir + "/duplicatesWithCompositeKeySingleton20180316.tsv")
