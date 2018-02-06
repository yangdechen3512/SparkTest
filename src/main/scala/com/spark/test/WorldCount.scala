package com.spark.test

import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("WorldCount")
    //      .setJars(List("G:\\GitHub\\scalaTest\\out\\artifacts\\scalaTest_jar\\scalaTest.jar"))
    val sc = new SparkContext(conf)
    //    val input = args(0)
    val texts = sc.textFile("C:\\Users\\telin\\Desktop\\test\\test.txt").map(line => line.split(" "))
      //    val texts = sc.textFile("/user/ydcc/test.txt").map(line => line.split(" "))
      .flatMap(words => words.map(word => (word.replaceAll("[^A-Za-z]", ""), 1)))
    val counts = texts.reduceByKey(_ + _)
    counts.collect.foreach {
      case (word, num) =>
        println(word + " " + num.toString)
    }
  }
}
