package com.fund.flow.algorithm

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD

class RF(data:RDD[LabeledPoint]) extends Serializable{
  def run = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 100 // Use more in practice.
    val featureSubsetStrategy = "onethird" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 20
    val maxBins = 100
    RandomForest.trainRegressor(data, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  }
}
