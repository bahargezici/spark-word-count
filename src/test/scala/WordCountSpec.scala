import WordCount.{createDataSet, createFinalResultSet}
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

class WordCountSpec extends FunSuite with SparkSessionTestWrapper {

  val files = "t1.txt" :: "t2.txt" :: Nil
  val filePath = "./src/main/resources/data/"
  for (i <- 0 to files.length - 1) {
    createDataSet(spark, filePath, files(i), "resultSet".concat(i.toString))
  }

  createFinalResultSet(spark)

  def rdd = spark.sql("SELECT * FROM finalResultSet LIMIT 4").rdd

  def wordList = rdd.map(r => r(0)).take(2)

  def countList = rdd.map(r => r(1)).take(3)

  def file1CountList = rdd.map(r => r(2)).take(3)

  def file2CountList = rdd.map(r => r(3)).take(2)

  def resulRet1Count = spark.sql("SELECT * FROM resultSet0 WHERE value = "+ """"and"""").rdd.map(r => r(1)).take(5)

  test("Test for getting first word with max count") {
    assert(wordList(0).toString == "the"
      && countList(0).asInstanceOf[Long] == 84
      && file1CountList(0).asInstanceOf[Long] == 42
      && file2CountList(0).asInstanceOf[Long] == 42)
  }

  test("Test for counts from file1") {
    assert(resulRet1Count(0).asInstanceOf[Long] == file1CountList(2).asInstanceOf[Long])
  }

  test("Test for data is sorted by total count") {
    assert(countList(0).asInstanceOf[Long] >= countList(1).asInstanceOf[Long] && countList(1).asInstanceOf[Long] >= countList(2).asInstanceOf[Long])
  }
}