package org.dxer.spark.demo

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

case class Person(name: String, age: Int)

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("s").master("local").getOrCreate()

    val rdd = spark.sparkContext
      .parallelize(Array(("zhangsan", 15), ("lisi", 14), ("wangwu", 15)))

    /**
      * 手动指定schema
      */

    // 定义schema
    val schema = StructType(Array(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true)
    ))


    val rowRDD = rdd.map(x => Row(x._1, x._2))
    // 将personRDD转化为DataFrame
    var df = spark.createDataFrame(rowRDD, schema)
    df.show(false)

    // 隐式转换
    import spark.implicits._

    /**
      * Tuple RDD转换成DataFrame
      */

    // 未指定列，列默认名为`_1`,`_2`
    df = rdd.toDF()
    df.show(false)
    // 指定列名
    rdd.toDF("name", "age")
    df.show(false)

    /**
      * Case class RDD转换成DataFrame
      */

    // 将rdd转化为DataFrame
    val personRDD = rdd.map(x => Person(x._1, x._2))
    df = personRDD.toDF("name", "age")
    df.show(false)

    /**
      * RDD转DataSet
      */

    /**
      * Tuple RDD转Dataset
      */
    var dataset = rdd.toDS()
    dataset.show(false)
    dataset = spark.createDataset(rdd)
    dataset.show(false)

    /**
      * Case class RDD转Dataset
      */
    var dataset1 = personRDD.toDS()
    dataset1.show(false)
    dataset1 = spark.createDataset(personRDD)
    dataset1.show(false)

    /**
      * DataFrame转RDD
      */

    val rdd1 = df.rdd.map(row => (row.get(0), row.get(1)))
    rdd1.collect().foreach(println)

    /**
      * Dataset转RDD
      */
    val rdd2 = dataset.rdd
    rdd2.collect().foreach(println)


    /**
      * Dataset转DataFrame
      */
    val df2 = dataset.toDF()
    df2.show(false)

    /**
      * DataFrame转Dataset
      */
    val dataset2 = df.as[Person]
    dataset2.show(false)


    spark.stop()
  }
}
