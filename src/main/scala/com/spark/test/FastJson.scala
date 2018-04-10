package com.spark.test

object FastJson {
  def main(args: Array[String]): Unit = {
    case class User(id: Long, organization_id: Long, username: String, role_ids: String, locked: Boolean)

    val jsonstr = "{\"id\":1,\"organization_id\":1,\"username\":\"admin\",\"role_ids\":\"1,2,3,4\",\"locked\":false}"

//    println(JSON.parseObject(jsonstr, classOf[User])
  }
}
