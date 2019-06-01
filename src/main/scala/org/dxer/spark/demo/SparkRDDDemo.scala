package org.dxer.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkDemo")

    val sc = new SparkContext(conf)

    /**
      * RDD创建
      * 1. 读取外部数据集
      * 2. 在驱动器中对一个集合进行并行化
      */
    // 读取外部数据集
    val lines = sc.textFile("file:///d:/test.txt")

    // 在驱动器中对一个集合进行并行化
    val words = sc.parallelize(List("hello world", "spark", "hadoop", "spark sql", "spark streaming"))


    /**
      * 转换操作
      */
    // flatMap
    val rdd1 = words.flatMap(x => x.split(" "))

    // filter
    val rdd2 = rdd1.filter(_.contains("spark"))

    // sample
    val rdd3 = rdd1.sample(false, 0.3)

    // distinct
    val rdd4 = rdd1.distinct()

    // map
    val rdd5 = rdd2.map((_, 1))

    val a = sc.parallelize(List(1, 2, 3))
    val b = sc.parallelize(List(3, 4, 5))

    // union：生成一个包含两个RDD中所有元素的RDD
    val c = a.union(b)
    // result: {1, 2, 3, 3, 4, 5}

    // intersection：求两个RDD共同的元素的RDD
    val d = a.intersection(b)
    // result: {3}

    // subtract：移除一个RDD中的内容
    val e = a.subtract(b)
    // result: {1, 2}

    // cartesian：与另一个RDD的笛卡尔积
    val f = a.cartesian(b)
    // result: {(1,3),(1,4),(1,5),(2,3),(2,4),(2,5),(3,3),(3,4),(3,5)}

    /**
      * 行动操作
      */

    val rdd = sc.parallelize(List(1, 2, 3, 3))

    // collect：返回RDD中的所有元素
    rdd.collect()
    // result: {1, 2, 3, 3}

    // count：返回RDD中的元素个数
    rdd.count()
    // result: 4

    // countByValue：返回个元素在RDD中出现的次数
    rdd.countByValue()
    // result: {(1,1),(2,1),(3,2)}

    // take：从RDD中返回2个元素
    rdd.take(2)
    // result: {1,2}

    // top：从RDD中返回最前面的2个元素
    val x = rdd.top(2)
    // result: {3,3}

    // takeOrdered：从RDD中按照提供的顺序返回最前面的2个元素
    rdd.takeOrdered(2)
    // result: {3,3}

    object Ord extends Ordering[Int] {
      override def compare(x: Int, y: Int): Int = {
        if (x < y) 1 else -1;
      }
    }
    val pa = sc.parallelize(Array(1, 2, 3, 4, 5, 6))
    rdd.takeOrdered(3)(Ord)

    // foreach(func)：对RDD中的每个元素使用给定的函数
    rdd.foreach(println)
    // result:
    // 1
    // 2
    // 3
    // 3

    /**
      * pair RDD转化操作
      */

    val pairRDD = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    val pairRDD1 = sc.parallelize(List((3, 5)))

    // reduceByKey: 合并具有相同键的RDD
    val rdd6 = pairRDD.reduceByKey((x, y) => x + y)

    // groupByKey: 对具有相同键的值进行分组
    val rdd7 = pairRDD.groupByKey()

    //    pairRDD.combineByKey()
    val rdd8 = pairRDD.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // keys: 返回一个仅包含键的RDD
    val rdd9 = pairRDD.keys

    // values: 返回一个仅包含值的RDD
    var rdd10 = pairRDD.values

    // sortByKey: 返回一个根据键排序的RDD
    val rdd11 = pairRDD.sortByKey()

    // subtract: 删除RDD中键与pairRDD1中键相同的元素
    var rdd12 = pairRDD.subtract(pairRDD1)

    // join: 对两个RDD进行内连接
    var rdd13 = pairRDD.join(pairRDD1)

    // rightOuterJoin: 对两个RDD进行连接操作，确保第一个RDD的键必须存在
    var rdd14 = pairRDD.rightOuterJoin(pairRDD1)

    // leftOuterJoin: 对两个RDD进行连接操作，确保第二个RDD的键必须存在
    var rdd15 = pairRDD.leftOuterJoin(pairRDD1)

    // cogroup: 将两个RDD中拥有相同键的数据分组到一起
    var rdd16 = pairRDD.cogroup(pairRDD1)

    /**
      * pair RDD行动操作
      */

    // countByValue: 对每个键对应的元素分别计数
    pairRDD.countByValue()

    // collectAsMap: 将结果以映射表的形式返回，以便查询
    pairRDD.collectAsMap()

    // lookup: 返回指定键对应的所有值
    pairRDD.lookup(3)

    sc.stop()
  }
}
