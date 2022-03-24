package com.rating.spark

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class Movie_Rating {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Movie_Rating")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val movieData: DataFrame = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("movie_metadata.csv")

  def meanRating() : Any = {
    val x: DataFrame = movieData.select( "imdb_score").agg(functions.avg("imdb_score"))
    val ratingForAll = x.collect()(0).get(0)
    ratingForAll
  }


  def stDev(): Any = {
    val x: DataFrame = movieData.select("imdb_score").agg(functions.stddev("imdb_score"))
    val standardDeviation = x.collect()(0).get(0)
    standardDeviation
  }

}

object Movie_RatingObj extends Movie_Rating
{
  def main(args:Array[String])
  {
    val y = new Movie_Rating()
    y.meanRating()
    y.stDev()
  }
}
