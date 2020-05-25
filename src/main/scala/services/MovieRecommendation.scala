package services

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import services.Implicits.Rating

object MovieRecommendation extends App with RecWrapper{
  override def main(args: Array[String]): Unit = {

    def parseRating(str: String): Rating = {
      val fields = str.split(",")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }
    val data =  spark.read.textFile("C:\\Users\\dreho\\Projects\\Movie-rec-engine\\src\\main\\resources\\ratings.csv")
    val header = data.first()
    val ratings = data.filter(row => row!= header).map(parseRating).toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    println(als)
    print("hello")
    val model = als.fit(training)
    println(model)
    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    println(predictions)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    println(evaluator)

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    userRecs.orderBy("userId").show()

  }

}
