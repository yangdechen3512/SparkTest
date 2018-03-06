package com.spark.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]){
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(2, 3, 5, 7, 3))
    val result1 = data.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val result2 = data.reduce((x, y) => x + y)
    val result3 = data.countByValue()
    val result4 = {
      data.map(x => x * x).collect().mkString("-")
    }
    println("分区是：" + data.partitions.length)
    println("结果是：" + result1)
    println(result2)
    println(result3)
    println(result4)
    sc.stop()
    println(findFirst(Array("h","a","g"),"a"))
    val t = new Teacher(33, "Alice")
    println(t.age)
  }

  def findFirst(ss : Array[String], key: String): Int = {
    def loop(n: Int): Int =
      if (n >= ss.length) -1
      else if (ss(n) == key) n
      else loop(n + 1)

    loop(0)
  }

}

class Teacher{
  var age:Int = _
  var name: String = _

  def this(age:Int,name:String){
    this()
    this.age = age
    this.name = name
  }
}


