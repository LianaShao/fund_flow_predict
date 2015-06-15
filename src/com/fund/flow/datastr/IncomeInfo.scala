package com.fund.flow.datastr

class IncomeInfo(record:String) extends Serializable {
  val date = record.split(",")(0)
  val mfdDailyYield = record.split(",")(1).toDouble
  val mfd7dailyYield = record.split(",")(2).toDouble
}
