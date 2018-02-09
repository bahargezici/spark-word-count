import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{count, desc}

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = setUp

    val files = "t1.txt" :: "t2.txt" :: Nil
    for( i <- 0 to files.length-1){
      createDataSet(sparkSession, files(i), "resultSet".concat(i.toString))
    }

    sparkSession.sql("SELECT NVL(a.value, b.value) as word, (NVL(a.count,0) + NVL(b.count,0)) as total_occurrences, " +
                    "NVL(a.count,0) as occurrences_file1, NVL(b.count,0) as occurrences_file2 " +
                    "FROM resultSet0 a " +
                    "FULL OUTER JOIN resultSet1 b " +
                    "ON a.value = b.value " +
                    "ORDER BY 2 DESC")
                .show()

  }

  /** Creates a dataset with a given filename, SparkSession and datasetName
    *
    *  @param sparkSession
    *  @param fileName
    *  @param datasetName
    */
  protected def createDataSet(sparkSession: SparkSession, fileName: String, datasetName: String) = {
    import sparkSession.implicits._
    val ds = sparkSession.read.text("./src/main/resources/data/" + fileName).as[String]
    val result = ds
      .flatMap(_.trim.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+")) // Split on whitespace
      .filter(_ != "") // Filter empty words
      .toDF() // to DataFrame
      .groupBy("value") // Group by word
      .agg(count("*") as "count") // Number of occurences of each word
      .orderBy(desc("count"))
    result.createOrReplaceTempView(datasetName)
  }

  /** Creates Spark Session and sets conf
    * Whereas in Spark 2.0 the same effects can be achieved through SparkSession,
    * without expliciting creating SparkConf, SparkContext or SQLContext,
    * as they’re encapsulated within the SparkSession
    *
    */
  private def setUp = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("WordCount")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.shuffle.partitions", 2)
    sparkSession.conf.set("spark.executor.memory", "2g")
    sparkSession
  }
}
