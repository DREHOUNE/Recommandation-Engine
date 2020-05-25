package services

import org.apache.spark.sql.{Encoder, Encoders}

object Implicits {
  implicit val encoder1: Encoder[Rating] = Encoders.product[Rating]
  implicit val encoder2: Encoder[movie] = Encoders.product[movie]
  implicit val encoder3: Encoder[exp] = Encoders.product[exp]
  implicit val encoder4: Encoder[moderate] = Encoders.product[moderate]


  case class Rating(userId: Int,
                    movieId: Int,
                    rating: Float,
                    timestamp: Long
                   )

  case class movie(movieId: Int,
                   title: String,
                   genres: String

                  )

  case class exp(title: String,
                 max_ratings: Double,
                 min_ratings: Double,
                 count_usr: Int

                )

  case class moderate(userId: Int,
                      movieId: Int,
                      rating: Double,
                      timestamp: Float

                     )

}