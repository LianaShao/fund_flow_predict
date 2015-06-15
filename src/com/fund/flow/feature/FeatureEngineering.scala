package com.fund.flow.feature

import java.text.SimpleDateFormat
import java.util.Calendar

import com.fund.flow.datastr.{ShiborInfo, IncomeInfo, UserTransInfo}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class FeatureEngineering(trans:RDD[(String,List[UserTransInfo])]
                         //income:RDD[(String,IncomeInfo)],
                        // shibor:RDD[(String,ShiborInfo)]
                        ) extends Serializable{
  //日期计算
  private def stringToDate(time:String):Calendar = {
    val formatter = new SimpleDateFormat("yyyyMMdd");
    val cal = Calendar.getInstance()
    cal.setTime(formatter.parse(time))
    cal
  }
  val transFeatures = trans.map{
    case (date,records) => {
      val buyTotal = records.map(_.totalPurchaseAmt).sum
      val redeemTotal = records.map(_.totalRedeemAmt).sum
      (date,(buyTotal,redeemTotal))
    }
  }.collect().sortBy(_._1)

  def runPurchase() = {
    for(i <- 7 until transFeatures.length) yield {
      val features = ArrayBuffer[Double]()
      val month = stringToDate(transFeatures(i)._1).get(Calendar.MONTH) + 1
      val dayOfMonth = stringToDate(transFeatures(i)._1).get(Calendar.DAY_OF_MONTH)
      val dayOfweek = stringToDate(transFeatures(i)._1).get(Calendar.DAY_OF_WEEK)
      features.+=(month,dayOfMonth,dayOfweek)

      var sum1,sum3 = 0.0;
      for(j <- i - 7 until i) {
        sum1 += transFeatures(j)._2._1.toDouble
        features.+=(transFeatures(j)._2._1.toDouble)
      }//最近7天的买入数据
      for(j <- i - 7 until i) {
        sum3 += transFeatures(j)._2._1.toDouble - transFeatures(j)._2._2.toDouble
       // features.+=(transFeatures(j)._2._1.toDouble - transFeatures(j)._2._2.toDouble)
      } //最近7天的买入和卖出的差
      features.+= (sum1 / 7, sum3 / 7)
      (features.toArray,transFeatures(i)._2._1)
    }
  }

  def runRedeem() = {
    for(i <- 7 until transFeatures.length) yield {
      val features = ArrayBuffer[Double]()
      val month = stringToDate(transFeatures(i)._1).get(Calendar.MONTH) + 1
      val dayOfMonth = stringToDate(transFeatures(i)._1).get(Calendar.DAY_OF_MONTH)
      val dayOfweek = stringToDate(transFeatures(i)._1).get(Calendar.DAY_OF_WEEK)
      features.+=(month,dayOfMonth,dayOfweek)

      var sum2,sum3 = 0.0;
      for(j <- i - 7 until i) {
        sum2 += transFeatures(j)._2._2.toDouble
        features.+=(transFeatures(j)._2._2.toDouble)
      }//最近7天的卖出数据
      for(j <- i - 7 until i) {
        sum3 += transFeatures(j)._2._1.toDouble - transFeatures(j)._2._2.toDouble
        //features.+=(transFeatures(j)._2._1.toDouble - transFeatures(j)._2._2.toDouble)
      } //最近7天的买入和卖出的差
      features.+= ( sum2 / 7, sum3 / 7)
      (features.toArray,transFeatures(i)._2._2)
    }
  }
}
