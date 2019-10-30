package pr

import org.apache.log4j.LogManager
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders


object PageRank {

  // Creating a case class for defining edges.
  final case class Graph (
                          V1:Int,
                          V2:Int
                        )

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
//    if (args.length < 2) {
//      logger.error("Usage:\nfc.PageRank  ------- ")
//      System.exit(1)
//    }

    // Delete output directory, only to ease local development; will not work on AWS. ===========
//        val hadoopConf = new org.apache.hadoop.conf.Configuration
//        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//        try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

//  creating a SparkSession
    val spark = SparkSession.builder()
                            .master("local[*]")  // for local dev remove on aws
                            .appName("PageRank")
                            .getOrCreate()

    generateGraphRDD(3,spark).foreach(println)

    val links = spark.sparkContext.objectFile[(String, Seq[String])]("links").partitionBy(new HashPartitioner(100)).persist()
    // Initialize each page's rank to 1.0; since we use mapValues, the resulting RDD
    // will have the same partitioner as links
     var ranks = links.mapValues(v => 1.0)
    // Run 10 iterations of PageRank
     for (i <- 0 until 10) {
       val contributions = links.join(ranks).flatMap{
         case (pageId, (links, rank)) => links.map(dest=> (dest, rank / links.size))
       }
       ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
     }// Write out the final ranks
     ranks.saveAsTextFile("ranks")

  }

  def generateGraph(k:Int): Seq[(Int,Int)] = {
    // Init empty scala collection
    var G:Seq[(Int,Int)] = Seq.empty
    for (i <- 1 to k*k) {
      if (i % k != 0) {
        G = G :+ (i, i + 1)
      }
      else {G = G :+ (i, 0)}
    }
    return G
  }


}
