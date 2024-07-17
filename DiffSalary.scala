package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DiffSalary {
  def loadData(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").option("inferschema", "true").csv(path)
  }

  // Affiche toutes les colonnes triées par salaire et ajoute une colonne diff avec le calcul du salaire le plus haut - salaire
  def DifféreSalaryBySalary(data: DataFrame): DataFrame = {
    val maxSalary = data.agg(max("salary").alias("max_salary")).collect()(0).getAs[Double]("max_salary")

    data
      .orderBy(desc("salary"))
      .withColumn("diff", lit(maxSalary) - col("salary"))
  }

}
