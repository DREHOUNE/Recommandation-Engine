package services

import org.apache.spark.sql.{DataFrame, Dataset}
import services.Implicits._

object DataExp extends RecWrapper {
  def GetCulumns(input1: Dataset[movie], input2: Dataset[moderate]): DataFrame = {

    input1.createOrReplaceTempView("movies")
    input2.createOrReplaceTempView("ratings")

    spark.sql("select movies.title, movierates.max_ratings, movierates.min_ratings, movierates.count_usr "
      + "from(SELECT ratings.movieId,max(ratings.rating) as max_ratings,"
      + "min(ratings.rating) as min_ratings,"
      + "count(distinct userId) as count_usr "
      + "FROM ratings group by ratings.movieId) movierates "
      + "join movies on movierates.movieId = movies.movieId "
      + "order by movierates.count_usr desc")

  }

}
