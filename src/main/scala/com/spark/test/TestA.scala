package com.spark.test


object TestA {
  def main(args: Array[String]): Unit = {
    for (c <- "hello") println(c)
    println(new User("Y", 23))
  }

  class User(name: String, age: Int) {
    override def toString: String = "name = " + this.name + ", age = " + this.age
  }

}
