package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import part2dataframes.ColumnsAndExpressions.movies2ColumnsDataFrames

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  //allCountriesDF.show()

  /**
    * Exercices
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + US_DVD_Sales
    * 3. Select all Comedy movies where IMDB Rating above 6
    *
    * Use as many versions as possible
    */
  // 1.
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  val movies2ColumnsDataFrames = moviesDF.select("Title", "Production_Budget")
  val movies2ColumnsDataFrames2 = moviesDF.select(
    'Title,
    col("Production_Budget") as ("Production budget")
  )
  movies2ColumnsDataFrames.show()

  // 2.
  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )
  moviesProfitDF.show()

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )
  moviesProfitDF2.show()

  val moviesProfitDF3 = moviesDF
    .select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))
  moviesProfitDF3.show()

  // 3.
  val comedyMoviesDF = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  comedyMoviesDF.show()
  println(s"Comedy movies number: ${comedyMoviesDF.count()}")

  val comedyMoviesDF2 = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)
  comedyMoviesDF2.show()
  println(s"Comedy movies number: ${comedyMoviesDF2.count()}")

  val comedyMoviesDF3 = moviesDF
    .select("Title", "IMDB_Rating")
    .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comedyMoviesDF3.show()
  println(s"Comedy movies number: ${comedyMoviesDF3.count()}")
  //moviesDataFrames.show()
}
