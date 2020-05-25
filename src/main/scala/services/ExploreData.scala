package services

object ExploreData extends App with RecWrapper{

  override def main(args: Array[String]): Unit = {

    val moviesDF = spark.read.option("header", "true").csv("C:\\Users\\dreho\\Projects\\Recommendation Engine with Spark\\src\\main\\resources\\movies.csv")
    val ratingsDF = spark.read.option("header", "true").csv("C:\\Users\\dreho\\Projects\\Recommendation Engine with Spark\\src\\main\\resources\\ratings.csv")

    moviesDF.show()
    ratingsDF.show()

    //val moviesRDD = moviesDF.rdd
    //val ratingsRDD = ratingsDF.rdd

    ratingsDF.createOrReplaceTempView("ratings")
    moviesDF.createOrReplaceTempView("movies")

    val numRatings = ratingsDF.count()
    val numUsers = ratingsDF.select(ratingsDF.col("userId")).distinct().count()
    val numMovies = ratingsDF.select(ratingsDF.col("movieId")).distinct().count()

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    // Get the max, min ratings along with the count of users who have rated a movie
    val results = spark.sql("select movies.title, movierates.max_ratings, movierates.min_ratings, movierates.count_usr "
      + "from(SELECT ratings.movieId,max(ratings.rating) as max_ratings,"
      + "min(ratings.rating) as min_ratings,"
      + "count(distinct userId) as count_usr "
      + "FROM ratings group by ratings.movieId) movierates "
      + "join movies on movierates.movieId = movies.movieId "
      + "order by movierates.count_usr desc")
    results.show()

    val mostActiveUsers = spark.sql("SELECT ratings.userId, count(*) as cnt from ratings "
      + "group by ratings.userId order by cnt desc limit 10")
    mostActiveUsers.show(false)

    // Find the movies that user 123100 rated higher than 4
    val results2 = spark.sql(
      """SELECT ratings.userId, ratings.movieId,
    ratings.rating, movies.title FROM ratings JOIN movies
    ON movies.movieId=ratings.movieId
    where ratings.userId = 123100 and ratings.rating > 4""")
    results2.show()


  }

}
