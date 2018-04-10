package com.spark.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamFlume {
  //  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  //  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
        ScalaFlumeEventTest("node2",4547)
//    ScalaFlumeEventPollTest("node2", 4546)
  }

  def ScalaFlumeEventTest(host: String, port: Int) {
    val conf = new SparkConf().setMaster("yarn-client").setAppName("FlumeEventCount")
    val ssc = {
      new StreamingContext(conf, Seconds(10))
    }
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER)

    //    stream.count().map(cnt => "Received " + cnt + " flume events.").print()
    stream.map(x => new String(x.event.getBody.array()))
      .flatMap(x => x.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

  def ScalaFlumeEventPollTest(host: String, port: Int): Unit = {
    val conf = new SparkConf().setMaster("yarn-client").setAppName("FlumeEventCount")
    val ssc = {
      new StreamingContext(conf, Seconds(15))
    }
    val stream = FlumeUtils.createPollingStream(ssc, host, port)

    //    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    stream.map(x => new String(x.event.getBody.array()))
      .flatMap(x => x.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

}