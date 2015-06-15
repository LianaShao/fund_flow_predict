package com.fund.flow.datastr

class UserTransInfo(record:String) extends Serializable{
  val userId= record.split(",")(0)
  val date = record.split(",")(1)
  val tBalance = record.split(",")(2).toLong
  val yBalance = record.split(",")(3).toLong
  val totalPurchaseAmt = record.split(",")(4).toLong
  val directPurchaseAmt = record.split(",")(5).toLong
  val purchaseBalAmt = record.split(",")(6).toLong
  val purchaseBankAmt = record.split(",")(7).toLong
  val totalRedeemAmt = record.split(",")(8).toLong
  val consumeAmt = record.split(",")(9).toLong
  val transferAmt = record.split(",")(10).toLong
  val tftobalAmt = record.split(",")(11).toLong
  val tftocardAmt = record.split(",")(12).toLong
  val shareAmt = record.split(",")(13).toLong
  var category1 = 0L
  var category2 = 0L
  var category3 = 0L
  var category5 = 0L
  if(record.split(",").length == 18){
    category1 = record.split(",")(14).toLong
    category2 = record.split(",")(15).toLong
    category3 = record.split(",")(16).toLong
    category5 = record.split(",")(17).toLong
  }

}
