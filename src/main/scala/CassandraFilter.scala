package simpl_spark_cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

class CassandraFilter(val sc_conf: SparkConf) {
  var conf: SparkConf = sc_conf

 def execute() {
   val sc = new SparkContext("local", "Simple App", conf)
   val sqlContext = new SQLContext(sc)
   val rdd = sc.cassandraTable("simpl_events_production", "api_events")
                .where("event_timestamp > ?", "f997eb26-d90f-11e5-853e-87daeae0332b")
                .where("event_timestamp < ?", "19968808-d910-11e5-a210-7587e4a4bbc8")
                .count()

   println(">>>>>>>>>>>>>>>>>>>>>")
   println(rdd)
   println(">>>>>>>>>>>>>>>>>>>>>")
   sc.stop()
 }
}
