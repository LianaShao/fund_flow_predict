package com.fund.flow

import com.fund.flow.algorithm.{GBDT, RF}
import com.fund.flow.base.BaseComputing
import com.fund.flow.feature.FeatureEngineering
import org.apache.spark.SparkContext

object FundFlow {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val data_trans = sc.textFile("/data/tianchi2/user_balance_table.csv").filter(!_.contains("report_date"))
    val trans = BaseComputing.getUserTransInfoByDate(data_trans)
    val features = new FeatureEngineering(trans).runPurchase().toArray

    val data = BaseComputing.toLabelPoint(sc,features);
    val splits = data.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    trainingData.cache()
    val model = new RF(trainingData).run
    //val model = new GBDT(trainingData).run
    val labelsAndPredictions = BaseComputing.predict(trainingData,model)
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.abs((v - p))/v.toDouble}.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression forest model:\n" + model.toDebugString)
  }

}
