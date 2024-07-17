package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object RunningTotal {
  def loadData(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").option("inferschema", "true").csv(path)
  }

  //ajout d'une colonne diff montre la différence entre les running_total quand le deuxième est au dessus de la ligne du dessus
  //running total ||diff
  // 10 || 10
  // 27 || 17
  // 15 || 15
  def Différencerunning_total(data: DataFrame): DataFrame ={
    data.groupBy("department")


  }
}
// colonne = "time;department;items_sold;running_total"
