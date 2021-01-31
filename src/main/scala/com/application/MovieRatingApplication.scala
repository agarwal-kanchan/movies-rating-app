package com.application

import com.service.MovieRatingService
import org.apache.spark.sql.SparkSession


object MovieRatingApplication {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("movie-rating-app")
      .getOrCreate()

    val movieRatingService = new MovieRatingService
    movieRatingService.process(args(0), spark)
    spark.stop()
  }

}
