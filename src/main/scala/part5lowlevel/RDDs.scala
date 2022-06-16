package part5lowlevel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable
import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers: immutable.Seq[Int] = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(company: String, date: String, price: Double)

  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // Drop the header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDDs = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))
  println(s"Stocks count = ${stocksRDDs.count()}")
  stocksRDDs.collect()
    .foreach(stockValue => println(stockValue))

  // 2B - reading from files
  val stocksRDDs2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0)) // To get rid of the header
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - Read from a DataFrame
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferShema", "true")
    .csv("src/main/resources/data/stocks.csv")
  // As we convert from a DataFrame, we will loose the type information => RDD[Row]
  val stocksRDDs3: RDD[Row] = stocksDF.rdd

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  // As we convert from a DataSet, we don't loose the type information  => RDD[StockValue]
  val stocksRDDs4: RDD[StockValue] = stocksDS.rdd

  // RDD -> DF
  val numbersDF: DataFrame = numbersRDD.toDF("numbers") // DataFrame: You loose the type information

  // RDD -> DS
  val numbersDS: Dataset[Int] = spark.createDataset(numbersRDD)   // DataSet: You keep the type information
}
