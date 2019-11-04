package pr

import org.apache.log4j.LogManager
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders


object PageRank {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
//    if (args.length < 2) {
//      logger.error("Usage:\nfc.PageRank  ------- ")
//      System.exit(1)
//    }

    // Delete output directory, only to ease local development; will not work on AWS. ===========
        val hadoopConf = new org.apache.hadoop.conf.Configuration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        try { hdfs.delete(new org.apache.hadoop.fs.Path("ranks"), true) } catch { case _: Throwable => {} }
    // ================

//  creating a SparkSession
    val spark = SparkSession.builder()
                            .master("local[*]")  // for local dev remove on aws
                            .appName("PageRank")
                            .getOrCreate()

    val partitioner = new HashPartitioner(1)

    val k = 100
    // creating graph rdd
    val graph = spark.sparkContext.parallelize(generateGraph(k))
                                  .partitionBy(partitioner).persist()

    val sink = spark.sparkContext.parallelize(List((0,0.0))).partitionBy(partitioner)

    val adj_graph = graph.groupByKey()
    // Initialize each page's rank;
    // Use of mapValues ensure similar partitioning
    var ranks = adj_graph.mapValues(v => 1.0/(k*k).toDouble).persist()

    //adding sink node and its pr to list
    ranks = ranks.union(sink)

    val num_nodes = adj_graph.count()
    var dist_pr:Double = 0.0

    //   Run 10 iterations of PageRank
     for (i <- 1 until 4) {
       var contributions = adj_graph.join(ranks).flatMap{
         case (v1, (v2, rank)) => v2.map(p => (p, rank / v2.size))
       }
       contributions = contributions.reduceByKey(_+_)
       // PageRank Mass Transfer
       val delta = contributions.lookup(0)
       val transferred_ranks = ranks.leftOuterJoin(contributions).filter(_._1 != 0)
         .mapValues{ case (v, new_v) => new_v.getOrElse(0.0) }

       if (delta.isEmpty) dist_pr = 0.0 else dist_pr = delta(0)/num_nodes

       logger.error(dist_pr.toString+ "iter: " +  i.toString)
       // transferred_ranks.saveAsTextFile("ranks"+i)
       ranks = transferred_ranks.mapValues(v => 0.15/num_nodes + 0.85D*(dist_pr+v))
     }
     logger.info("++++++++++++++++++< DebugString >++++++++++++++++++")
     logger.info(ranks.toDebugString)
     ranks.sortBy(_._1, true,numPartitions = 1).saveAsTextFile("ranks")

  }

  def generateGraph(k:Int): List[(Int,Int)] = {
    // Init empty scala collection
    var G:List[(Int,Int)] = List.empty
    for (i <- 1 to k*k) {
      if (i % k != 0) {
        G = G :+ (i, i + 1)
      }
      else {G = G :+ (i, 0)}
    }
    return G
  }


}
