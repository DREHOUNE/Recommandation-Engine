package repository

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import services.RecWrapper

object CassandraService extends App with RecWrapper{
  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "35.181.155.182")
      .setAppName("Spark MultipleContest Test")
      .set("spark.driver.allowMultipleContexts", "true")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Create a Java Context which is the same as the scala one under the hood
    JavaSparkContext.fromSparkContext(sc)

    val moviesDF = spark.read.option("header", "true").csv("C:\\Users\\dreho\\Projects\\Movie-rec-engine\\src\\main\\resources\\movies.csv")
    val ratingsDF = spark.read.option("header", "true").csv( "C:\\Users\\dreho\\Projects\\Movie-rec-engine\\src\\main\\resources\\ratings.csv")

    val connector = CassandraConnector(spark.sparkContext)
    println(connector.conf.hosts)
    connector.withSessionDo(session => {
      println(session.getState)
      session.execute("DROP KEYSPACE IF EXISTS movies")
      session.execute("CREATE KEYSPACE Movies WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute("USE movies")
      session.execute("CREATE TABLE data(movieid text PRIMARY KEY, titre text ,genre text, userid text, rating text, timestamp varint)")
    }
    )

    val data = moviesDF.join(ratingsDF, "movieId")
    //data.rdd.saveToCassandra("movies", "data", SomeColumns("movieid", "titre","genre","userid","rating","timestamp"))
    data.rdd.saveToCassandra("movies", "data", SomeColumns("movieid", "titre","genre","userid","rating","timestamp"))

    def cassandraDataTrain(): Unit ={
      val datafortrain = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "data", "keyspace" -> "movies"))
        .load.select("movieid", "userid", "rating", "timestamp").toDF()
    }

  }

}
