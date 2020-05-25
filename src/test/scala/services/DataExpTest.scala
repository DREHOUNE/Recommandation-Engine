package services

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.FlatSpec
import services.Implicits._

class DataExpTest extends FlatSpec {
  val spark : SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "Get the max, min ratings along with the count of users who have rated a movie" should "ok" in {

    //Given
    val input1: Dataset[movie] = Seq(
      (1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
      (2, "Jumanji (1995)", "Adventure|Children|Fantasy")
    ).toDF("movieId", "title", "genres").as[movie]

    val input2: Dataset[moderate] = Seq(
      (1, 1, 4.0, 964982703),
      (4, 1, 5.0, 964981247),
      (1, 2, 0.5, 964982224),
      (3, 1, 5.0, 964983815),
      (5, 2, 3.0, 964982931)

    ).toDF("userId", "movieId", "rating" ,"timestamp").as[moderate]


    val expected: DataFrame = Seq(
      ("Toy Story (1995)", 5.0, 4.0, 3),
      ("Jumanji (1995)", 3.0, 0.5, 2)
    ).toDF("title", "max_ratings", "min_ratings", "count_usr")

    //When
    val result = DataExp.GetCulumns(input1, input2)
    println("result")
    result.show()

    println("expected")
    expected.show()

    //Then
    assert(expected.collectAsList()==result.collectAsList())
  }


}
