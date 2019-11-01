package pr

import org.apache.log4j.LogManager
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession


object PageRank2 {

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

    // creating graph rdd
    val graph = spark.sparkContext.parallelize(generateGraph(3))
                                  .partitionBy(partitioner).persist()

    val adj_graph = graph.groupByKey()

    val sink = spark.sparkContext.parallelize(List((0,0.0))).partitionBy(partitioner)

    // Initialize each page's rank;
    // Use of mapValues ensure similar partitioning
    var ranks = graph.mapValues(v => 1.0/9.0)

    //adding sink node and its pr to list
    ranks = ranks.union(sink)

    val num_nodes = ranks.count()
    var dist_pr:Double = 0.0
    var output = 0
    //     Run 10 iterations of PageRank
     for (i <- 0 until 10) {
       val delta = ranks.lookup(0)
       if (delta.isEmpty){dist_pr = 0.0} else {dist_pr = delta(0)/num_nodes}
       val contributions = adj_graph.join(ranks).flatMap{
         case (v1, (v2, rank)) => v2.map(p => (p, rank / v2.size))
       }
       ranks = contributions.reduceByKey((x,y)=> x+y).mapValues(v => 0.15/num_nodes + 0.85*(dist_pr+v))
     }
    // Write out the final ranks
     ranks.saveAsTextFile("ranks")
//    print("\n+++++++++++++++++++++++++++++++++++++++++++++++\n")
//    logger.info(ranks.collect().foreach(println))
//    print("\n+++++++++++++++++++++++++++++++++++++++++++++++\n")

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
