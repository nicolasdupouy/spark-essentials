package part2dataframes

import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")) //.show()

  /**
   * Exercices
   *
   * 1. Show all employees and their max salary
   * 2. Show all employees who were never managers
   */
  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName").load()

  val salariesDF = readTable("salaries")
  val employeesDF = readTable("employees")
  val departmentManagerDF = readTable("dept_manager")

  // 1. Show all employees and their max salary
  // Get the max salary for each employee
  val maxSalaryByEmployeeDF = salariesDF
    .groupBy(col("emp_no"))
    .max("salary")

  employeesDF.join(
    maxSalaryByEmployeeDF,
    employeesDF.col("emp_no") === maxSalaryByEmployeeDF.col("emp_no"),
    Inner.sql)
    .show()

  // 2. Show all employees who were never managers
  val employeesWhoWereNeverManagers = employeesDF.join(
    departmentManagerDF,
    employeesDF.col("emp_no") === departmentManagerDF.col("emp_no"),
    "left_anti")
    .select(col("first_name"), col("last_name"))
    .orderBy(col("first_name"))
  println(s"employees who were never managers: #${employeesWhoWereNeverManagers.count()}")
  employeesWhoWereNeverManagers.show()
}
