package org.dxer.spark.demo


import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    //    conf.set("spark.default.parallelism", "10")

    val sc = new SparkContext(conf)

    //    Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)

    val lines = sc.textFile("data/dataset.txt")

    println(lines.getNumPartitions)

    val result = lines.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    //    println(result.toDebugString)

    println(result.getNumPartitions)

    result.collect()
      .foreach(x => println(x._1 + " = " + x._2))

    Thread.sleep(1000 * 60 * 60)
    sc.stop()
  }
}
