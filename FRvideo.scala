package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FRvideo {
  def loadData(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header", "true").option("inferschema", "true").csv(path)
  }

  def topvideotrending(data: DataFrame): DataFrame = {
    data.groupBy("channel_title")
      .agg(count("*").alias("count"))
      .withColumn("percentage", format_string("%.1f%%", col("count") * 100 / sum("count").over()))
      .orderBy(desc("count"))
  }

  def ratioLikedislikeByChaine(data: DataFrame): DataFrame = {
    data.groupBy("channel_title")
      .agg(
        sum("likes").alias("total_likes"),
        sum("dislikes").alias("total_dislikes")
      )
      .withColumn("like_dislike_ratio", col("total_likes") / col("total_dislikes"))
      .select("channel_title", "like_dislike_ratio")
      .orderBy(desc("like_dislike_ratio"))
  }
}