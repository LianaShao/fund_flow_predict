package com.fund.flow.base

import com.fund.flow.datastr.{ShiborInfo, IncomeInfo, UserTransInfo}
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD

object BaseComputing {

  def getUserTransInfoByDate(data:RDD[String]):RDD[(String,List[UserTransInfo])] = {
    data.map{
      case record => {
        val date = record.split(",")(1)
        (date,record)
      }
    }.groupByKey().map{
      case (date,records) => {
        (date,records.toList.map(new UserTransInfo(_)))
      }
    }
  }

  def toLabelPoint(sc:SparkContext,data:Array[(Array[Double],Long)])={
    val rdd = sc.parallelize(data);
    rdd.map{
      case (features,label) => {
        new LabeledPoint(label.toDouble,Vectors.dense(features))
      }
    }
  }

  def predict(data:RDD[LabeledPoint],model:RandomForestModel) = {
    data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
  }
  def predict(data:RDD[LabeledPoint],model:GradientBoostedTreesModel) = {
    data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
  }

//  def getIncomeInfoByDate(data:RDD[String]):RDD[(String,IncomeInfo)] = {
//    data.map{
//      case record => {
//        val date = record.split(",")(0)
//        (date,new IncomeInfo(record))
//      }
//    }
//  }
//
//  def getShiborInfoByDate(data:RDD[String]):RDD[(String,ShiborInfo)] = {
//    data.map {
//      case record => {
//        val date = record.split(",")(0)
//        (date, new ShiborInfo(record))
//      }
//    }
//  }
}
