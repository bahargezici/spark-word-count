import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

class WordCountSpec extends FunSuite with SparkSessionTestWrapper{

  test("Test for getting the word with max count"){

    assert()
  }
}