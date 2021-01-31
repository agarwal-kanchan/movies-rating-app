package com.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CommonUtils {

  def readFileToDataFrame(path: String, spark: SparkSession): DataFrame = {
    val dataFrame = spark.read.options(Map("inferSchema" -> "true", "sep" -> "::", "header" -> "false")).csv(path)
    dataFrame
  }

  def writeDataFrameToFile(dataFrame: DataFrame, path: String, spark: SparkSession): Unit = {
    dataFrame.coalesce(1).write.mode(SaveMode.Overwrite).format("parquet").parquet(path)
  }
}
