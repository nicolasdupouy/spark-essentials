package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // All the values except null
  moviesDF.selectExpr("count(Major_Genre)") // equivalent

  // Counting all
  moviesDF.select(count("*")) //.show() // count all the rows and INCLUDE nulls

  // Counting distinct
  val genresCountDistinct = moviesDF.select(countDistinct("Major_Genre"))
  //genresCountDistinct.show()

  // Approximate count
  moviesDF.select(approx_count_distinct("Major_Genre")) //.show()
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) //.show()

  // min and max
  moviesDF.select(min(col("IMDB_Rating"))) //.show()
  moviesDF.selectExpr("min(IMDB_Rating)") //.show()

 // sum
  moviesDF.select(sum(col("US_Gross"))) //.show()
  moviesDF.selectExpr("sum(US_Gross)")//.show()

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // Data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")) // standard deviation
  )//.show()

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // equivalent to "select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
    //.show()

  /**
    * Exercices
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating in the average US gross revenue PER DIRECTOR
    */

  // 1.
  moviesDF.select(sum(col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")))//.show()
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum(col("Total_Gross")))
    //.show()

  // 2.
  moviesDF.select(countDistinct(col("Director")))//.show()

  // 3.
  moviesDF
    .select(
      mean(col("US_Gross")),
      stddev(col("US_Gross"))
    )
  //.show()

  // 4.
  moviesDF
    .groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      avg("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
