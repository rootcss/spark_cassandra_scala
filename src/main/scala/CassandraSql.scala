package simpl_spark_cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

class CassandraSql(val sc_conf: SparkConf) {
  var conf: SparkConf = sc_conf

  def execute() {
    val sc = new SparkContext("local", "Simple App", conf)

    val rdd = sc.cassandraTable("simpl_events_production", "api_events")
    println(">>>>>>>>>>>>>>>>>>>>>")
    // val count = rdd.where("event_timestamp > ?", "f997eb26-d90f-11e5-853e-87daeae0332b")
    //                .where("event_timestamp < ?", "19968808-d910-11e5-a210-7587e4a4bbc8")
    //                .count()
    val c = rdd.spanBy(row => row.getString("bucket_id")).count()
    println(c)
    println(">>>>>>>>>>>>>>>>>>>>>")
    sc.stop()
  }

 def execute2() {
   val sc = new SparkContext("local", "Simple App", conf)
   val sqlContext = new SQLContext(sc)

   val uuidtestDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace"->"simpl_events_production","table"->"api_events")).load
   uuidtestDF.registerTempTable("api_events")
   val fromTS = org.apache.cassandra.utils.UUIDGen.getTimeUUID(System.currentTimeMillis)
   // val filtered = uuidtestDF.filter("bucket_id = 'date_2016_02_19'").filter("event_timestamp < '"+fromTS+"'").show
   val filteredDF = sqlContext.sql(s"select * from api_events where bucket_id = 'date_2016_02_19'")
   println(">>>>>>>>>>>>>>>>>>>>>")
   filteredDF.show()
   println(">>>>>>>>>>>>>>>>>>>>>")
   sc.stop()
 }
}


// sc.cassandraTable("test", "events")
//   .spanBy(row => (row.getInt("year"), row.getInt("month")))
//
// sc.cassandraTable("test", "events")
//   .keyBy(row => (row.getInt("year"), row.getInt("month")))
//   .spanByKey
