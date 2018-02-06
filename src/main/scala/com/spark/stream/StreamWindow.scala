package com.spark.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWindow {

  def main()(args: Array[String]): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName("StreamWindowTest")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("C:\\Users\\telin\\Desktop\\test")

    val lines = ssc.socketTextStream("localhost", 1111, StorageLevel.MEMORY_ONLY_2)// 服务器地址，端口
    val words = lines.flatMap(_.split(" ")).map(x => (x, 1))
    val windowedWordCounts = words.reduceByKeyAndWindow((a: Int, b: Int) => a + b,Seconds(15),Seconds(10))
    windowedWordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
