import WordCount.{createDataSet, createFinalResultSet}
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
//      .config("spark.default.parallelism", 12)
      .appName("spark session")
      .getOrCreate()
  }

}

class WordCountSpec extends FunSuite with SparkSessionTestWrapper{

  val files = "t1.txt" :: "t2.txt" :: Nil
  val filePath = "./src/main/resources/smalldata/"
  for (i <- 0 to files.length - 1) {
    createDataSet(spark, filePath, files(i), "resultSet".concat(i.toString))
  }

  createFinalResultSet(spark)

  def rdd = spark.sql("SELECT * FROM finalResultSet LIMIT 4").rdd

  def wordList[String] = rdd.map(r => r(0)).take(2)

  def countList[Long] = rdd.map(r => r(1)).take(3)

  def file1CountList[Long] = rdd.map(r => r(2)).take(3)

  def file2CountList[Long] = rdd.map(r => r(3)).take(2)

  def resulRet1Count[Long] = spark.sql("SELECT * FROM resultSet0 WHERE value = "+ """"bahar"""").rdd.map(r => r(1)).take(5)

  test("Test for getting first word with max count") {
    assert(wordList(0) == "patik"
      && countList(0) == 5
      && file1CountList(0) == 3
      && file2CountList(0) == 2)
  }

  test("Test for counts from file1") {
    assert(resulRet1Count(0).asInstanceOf[Long] == file1CountList(2).asInstanceOf[Long])
  }

  test("Test for data is sorted by total count") {
    assert(countList(0).asInstanceOf[Long] >= countList(1).asInstanceOf[Long] && countList(1).asInstanceOf[Long] >= countList(2).asInstanceOf[Long])
  }
  spark.close()
}