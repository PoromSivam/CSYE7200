import com.rating.spark.Movie_RatingObj
import org.apache.spark.sql.SparkSession
import org.json4s.scalap.Success
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfter, flatspec}

import scala.io.Source
import scala.util.Try

class Movie_RatingSpec extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter  {

    implicit var spark: SparkSession = _

    before {
      spark = SparkSession
        .builder()
        .appName("Movie_Rating")
        .master("local[*]")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
    }

    behavior of "Spark"

    it should "intake movie metadata valid path" taggedAs Slow in {
      val triedPath = Try(Source.fromResource("movie_metadata.csv"))
      triedPath.isSuccess shouldBe true
    }

    it should "standard deviation of all movies" taggedAs Slow in {
      Movie_RatingObj.stDev() should matchPattern {
        case 1.1251158657328193 =>
      }
    }


    it should "mean rating for all movies" taggedAs Slow in {
      Movie_RatingObj.meanRating() should matchPattern {
        case 6.442137616498111 =>
      }
    }
}


