package com.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamFlume {

  def main(args: Array[String]): Unit ={
    ScalaFlumeEventTest("192.168.88.202",4545)
  }
  def ScalaFlumeEventTest(host : String, port : Int) {
    val conf = new SparkConf().setMaster("yarn-client").setAppName("FlumeEventCount")
    val ssc = {
      new StreamingContext(conf, Seconds(10))
    }

    val stream = FlumeUtils.createStream(ssc, host ,port,StorageLevel.MEMORY_ONLY_SER)
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    ssc.start()
    ssc.awaitTermination()
  }
}