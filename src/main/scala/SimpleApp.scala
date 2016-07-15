import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

object SimpleApp {
 def main(args: Array[String]) {
   // keeping the logs quiet
   Logger.getLogger("org").setLevel(Level.ERROR)
   Logger.getLogger("akka").setLevel(Level.ERROR)

   val conf = new SparkConf().setAppName("Scala Cassandra Test").set("spark.cassandra.connection.host", "192.168.56.101")
   val sc = new SparkContext("local", "Simple App", conf)
   val sqlContext = new SQLContext(sc)

   val uuidtestDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace"->"simpl_events_production","table"->"api_events")).load
   uuidtestDF.registerTempTable("api_events")
   val fromTS = org.apache.cassandra.utils.UUIDGen.getTimeUUID(System.currentTimeMillis)
   // val filtered = uuidtestDF.filter("bucket_id = 'date_2016_02_19'").filter("event_timestamp < '"+fromTS+"'").show
   val filteredDF = sqlContext.sql(s"select * from api_events where bucket_id = 'date_2016_02_19'")
   filteredDF.show()
   sc.stop()
 }
}

// val rdd = sc.cassandraTable("simpl_events_production", "api_events")
// println(rdd.count)
