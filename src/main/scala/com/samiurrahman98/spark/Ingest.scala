package com.samiurrahman98.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, asc_nulls_first}
import org.apache.log4j._

object Ingest {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Ingest Attendance Data")
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    val rawData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("output/*/*.csv")
      .coalesce(4)

    // rawData.cache()

    // first drop all inapplicable data - missing data, etc.
    // println("Raw lines: " + rawData.count())

    val relevantData = rawData
      .na.drop("all", Seq("Attendance"))

    // need to be examined on a case-by-case basis to determine if data is applicable to study
    val notedData = relevantData
      .where("Notes IS NOT NULL")
      .orderBy(col("Date").asc_nulls_first, col("Start (ET)").asc_nulls_first)
      .coalesce(1)

    notedData.write
      .format("csv")
      .mode("overwrite")
      .option("header", "true")
      .option("sep", ",")
      .option("quoteAll", "true")
      .save("spark-output/noted")

    // println("Relevant lines: " + relevantData.count())
    // rawData.show()

    // rawData.unpersist()

    spark.stop()
  }
}
