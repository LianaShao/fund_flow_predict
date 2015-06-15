package com.fund.flow.datastr

class ShiborInfo(record:String) extends Serializable {
  val date = record.split(",")(0)
  val interestON = record.split(",")(1).toDouble
  val interest1W = record.split(",")(2).toDouble
  val interest2W = record.split(",")(3).toDouble
  val interest1M = record.split(",")(4).toDouble
  val interest3M = record.split(",")(5).toDouble
  val interest6M = record.split(",")(6).toDouble
  val interest9M = record.split(",")(7).toDouble
  val interest1Y = record.split(",")(8).toDouble
}
