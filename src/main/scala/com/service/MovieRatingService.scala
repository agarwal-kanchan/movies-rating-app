package com.service

import com.utils.CommonUtils
import com.config.SparkLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{desc, rank}
import org.slf4j.LoggerFactory

class MovieRatingService {

  def process(path: String): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
    val spark = new SparkLoader().spark
    try {

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
      logger.info("Movie Ratings Data frame")
      movieRatings.show(false)
      val users = utils.readFileToDataFrame(path + usersFile, spark).
        withColumnRenamed("_c0", "UserID").withColumnRenamed("_c1", "Gender").
        withColumnRenamed("_c2", "Age").withColumnRenamed("_c3", "Occupation").
        withColumnRenamed("_c4", "Zip-code")

      val usersOrderedRatings = ratings.withColumn("rank", rank().over(Window.partitionBy("UserId").orderBy(desc("rating"))))
        .orderBy("UserId")

      val allUsersTop3Movies = usersOrderedRatings.join(movies, Seq("MovieId"), "inner").filter(usersOrderedRatings("rank") <= 3)
        .drop("rank").drop("MovieId").drop("timestamp").drop("Genres").orderBy("UserId")
      logger.info("User Top 3 Movies Data Frame ")
      allUsersTop3Movies.show(false)
      utils.writeDataFrameToFile(movies, path + destDir + "_movies", spark)

      utils.writeDataFrameToFile(ratings, path + destDir + "_ratings", spark)

      utils.writeDataFrameToFile(users, path + destDir + "_users", spark)

      utils.writeDataFrameToFile(movieRatings, path + destDir + "_movieRatings", spark)

      utils.writeDataFrameToFile(allUsersTop3Movies, path + destDir + "_usersTop3Movies", spark)

    } catch {
      case ex: Exception => {
        logger.error("Exception while processing the data")
        logger.error(ex.printStackTrace().toString)
      }

    }
    finally {
      spark.stop()
    }
  }
}
