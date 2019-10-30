package pr

import org.apache.log4j.LogManager
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
    if (args.length < 2) {
      logger.error("Usage:\nfc.FollowerCount <edge-file input> <output dir> <node file (optional)>")
      System.exit(1)
    }

    // Delete output directory, only to ease local development; will not work on AWS. ===========
        val hadoopConf = new org.apache.hadoop.conf.Configuration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    //    creating a SparkSession
    val spark = SparkSession.builder()
                            .master("local[*]")  // for local dev remove on aws
                            .appName("PageRank")
                            .getOrCreate()

  }

}
