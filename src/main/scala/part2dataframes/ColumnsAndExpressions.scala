package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // Selecting (Technical term: projection)
  val carsNamesDF = carsDF.select(firstColumn)
  carsNamesDF.show()

  // Various select method
  import spark.implicits._
  val testSelectVarious = carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"), // alias of col
    'Year, // Scala Symbol, auto-converted to column (needs import spark.implicits._)
    $"Horsepower", // fancier interpolated string, returns a Column object (needs import spark.implicits._)
    expr("Origin") // EXPRESSION
  )
  testSelectVarious.show()

  // Select with plain column names
  val testSelectPlainColumnNames = carsDF.select("Name", "Year")
  testSelectPlainColumnNames.show()

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    simplestExpression,
    weightInKgExpression.as("Weight_in_kgs"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs computed by expr")
  )
  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )
  carsWithSelectExprWeightsDF.show()

  // DF processing
  // Adding a column
  val carsWithKgs3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // Rename a column
  val carsWithcolumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names (use backticks when renaming "Weight in pounds" instead of "Weight_in_pounds")
  carsWithcolumnRenamed.selectExpr("`Weight in pounds`")
  // Remove a column
  carsWithcolumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americalCarsDF = carsDF.filter("Origin = 'USA'")
  // Chain filters
  val americalPowerfulCarsDF = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)
  // or
  val americalPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americalPowerfulCarsDF2Infix = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150) // as "and" is infix
  val americalPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // Unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCars = carsDF.union(moreCarsDF) // works if the DFs have the same schemas

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()
}
