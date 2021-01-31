package com.application

import com.service.MovieRatingService

object MovieRatingApplication {

  def main(args: Array[String]): Unit = {
    val movieRatingService = new MovieRatingService
    movieRatingService.process(args(0))
  }

}
