package com.service

import com.utils.CommonUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{desc, rank}

class MovieRatingService {

  def process(path: String, spark: SparkSession): Unit = {

    val moviesFile = "movies.dat"
    val ratingsFile = "ratings.dat"
    val usersFile = "users.dat"

    val destDir = "result"

    val utils = new CommonUtils()
    val movies = utils.readFileToDataFrame(path + moviesFile, spark).
      withColumnRenamed("_c0", "MovieID").withColumnRenamed("_c1", "Title").
      withColumnRenamed("_c2", "Genres")

    val ratings = utils.readFileToDataFrame(path + ratingsFile, spark).
      withColumnRenamed("_c0", "UserID").withColumnRenamed("_c1", "MovieID").
      withColumnRenamed("_c2", "Rating").withColumnRenamed("_c3", "TimeStamp")

    val movieRatings = ratings.join(movies, Seq("MovieId"), "right").
      groupBy(movies("MovieId"), movies("Title"), movies("Genres")).agg(
      functions.max(ratings("rating")).as("max"),
      functions.min(ratings("rating")).as("min"),
      functions.avg(ratings("rating")).as("avg")
    ).orderBy("MovieId")

    val users = utils.readFileToDataFrame(path + usersFile, spark).
      withColumnRenamed("_c0", "UserID").withColumnRenamed("_c1", "Gender").
      withColumnRenamed("_c2", "Age").withColumnRenamed("_c3", "Occupation").
      withColumnRenamed("_c4", "Zip-code")

    val usersOrderedRatings = ratings.withColumn("rank", rank().over(Window.partitionBy("UserId").orderBy(desc("rating"))))
      .orderBy("UserId")

    val allUsersTop3Rating = usersOrderedRatings.join(movies, Seq("MovieId"), "inner").filter(usersOrderedRatings("rank") <= 3)
      .drop("rank").drop("MovieId").drop("timestamp").drop("Genres").orderBy("UserId")
    allUsersTop3Rating.show(false)


    utils.writeDataFrameToFile(movies, path + destDir, spark)

    utils.writeDataFrameToFile(ratings, path + destDir, spark)

    utils.writeDataFrameToFile(users, path + destDir, spark)

    utils.writeDataFrameToFile(movieRatings, path + destDir, spark)

    utils.writeDataFrameToFile(allUsersTop3Rating, path + destDir, spark)
  }
}
