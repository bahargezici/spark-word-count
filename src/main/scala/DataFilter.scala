package com.improvedigital
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFilter {

  def main(args: Array[String]): Unit = {

    /* Whereas in Spark 2.0 the same effects can be achieved through SparkSession,
       without expliciting creating SparkConf, SparkContext or SQLContext,
       as they’re encapsulated within the SparkSession */

    //Creating SparkSession
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._

    //set new runtime options
    sparkSession.conf.set("spark.sql.shuffle.partitions", 2)
    sparkSession.conf.set("spark.executor.memory", "2g")

    // Read text files in spark as DataSet[String]
    val ds = sparkSession.read.text("./src/main/resources/data").as[String]

    val result = ds
      .flatMap(_.trim.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+"))   // Split on whitespace
      .filter(_ != "")                                                         // Filter empty words
      .toDF()                                                                  // Convert to DataFrame to perform aggregation / sorting
      .groupBy("value")                                                        // Group by word
      .agg(count("*") as "numOccurances")                                      // Count number of occurences of each word
      .orderBy(desc("numOccurances"))                                          // Order by occurences descending

    result.show()
  }
}