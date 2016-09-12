package simpl_spark_cassandra

import simpl_spark_cassandra._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

object Main {
 def main(args: Array[String]) {
   Logger.getLogger("org").setLevel(Level.ERROR)
   Logger.getLogger("akka").setLevel(Level.ERROR)
   val conf = new SparkConf().setAppName("Scala Cassandra Test").set("spark.cassandra.connection.host", "192.168.56.101")

  //  val csql = new CassandraSql(conf)
  //  csql.execute()
  //  csql.execute2()

    val csql = new RabbitmqSpark()
    csql.execute()
 }
}
// val cfl = new CassandraFilter(conf)
// cfl.execute()
