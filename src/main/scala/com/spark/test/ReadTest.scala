package com.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object ReadTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("readTest")
    val sc = new SparkContext(conf)

    //    val data1 = sc.textFile("C:\\Users\\telin\\Desktop\\test\\test*.txt")
    val data2 = sc.wholeTextFiles("C:\\Users\\telin\\Desktop\\test")
    println(data2.getNumPartitions)

    //    data1.foreach(println)
    data2.foreach(println)

    //    val writer = new PrintWriter(new File("C:\\Users\\telin\\Desktop\\test\\test.txt"))
    //    writer.println(data2)
    //    writer.close()
  }
}
