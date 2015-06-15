package com.fund.flow.datastr

/**
 * Created by closure on 15/6/14.
 */
class UserInfo(record:String) extends Serializable{
  val userId = record.split(",")(0)
  val sex = record.split(",")(1).toInt
  val city = record.split(",")(2).toLong
  val constellation = record.split(",")(3)
}
