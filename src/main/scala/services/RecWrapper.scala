package services

import org.apache.spark.sql.SparkSession

trait RecWrapper {

  val spark: SparkSession= SparkSession
    .builder()
    .appName("MyApp")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "35.181.155.182")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

}
