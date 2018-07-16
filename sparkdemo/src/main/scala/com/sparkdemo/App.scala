package com.sparkdemorow

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

/**
* @author ${user.name}
*/
object App {
    def main(args : Array[String]) {
        // Set logging level to ERROR, default INFO is verbose
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        println( "Hello Spark!" )

        // Create Spark session
        val spark = SparkSession.builder
            .appName("spark app")
            .config("spark.master", "local")
            .getOrCreate()

        // RDD by parallelize
        val data = Array(1, 2, 3, 4, 5)
        val rdd = spark.sparkContext.parallelize(data)
        rdd.collect().foreach( value => println(value) )

        // Read CSV as DataSet<Row> (DataFrame)
        val file: File = new File("src/main/resources/test.csv");
        val df = spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(file.getPath)
        df.show()

        val dfToRDD = df.rdd
        println(dfToRDD.count())
    }
}
