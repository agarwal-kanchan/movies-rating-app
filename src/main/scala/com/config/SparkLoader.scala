package com.config

import org.apache.spark.sql.SparkSession

class SparkLoader {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("movie-rating-app")
    .getOrCreate()
}
