package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  // Joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner") // JoinType inner is default
  //guitaristsBandsDF.show()

  // Outer joins
  // Left outer join
  // => everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer") //.show()

  // Right outer join
  // => everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer") //.show()

  // Full outer join
  // => everything in the inner join + all the rows in BOTH table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer") //.show() // or "full_outer"

  // semi-joins
  // => everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi") //.show()

  // anti-joins
  // => everything in the left DF for which there is a NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti") //.show()

  // Things to bear in mind
  //guitaristsBandsDF.select("id", "band").show() // Ambiguous column

  // Option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band")) //.show()
  // Option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id")) //.show()
  // Option 3 - rename the offending column and keep the data
  val bandsModifiedDF = bandsDF.withColumnRenamed("id", "bandId") //.show()
  guitaristsDF.join(bandsModifiedDF, guitaristsDF.col("band") === bandsModifiedDF.col("bandId")) //.show()

  // Using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()
}
