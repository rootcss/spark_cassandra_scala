package simpl_spark_cassandra

import org.apache.spark.streaming.rabbitmq.RabbitMQUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.Time
import spray.json._

class RabbitmqSpark {

  def execute() {
    println("Creating Streaming context..")
    val conf = new SparkConf().setAppName("simpl-rabbitmq").setMaster("local[2]") // .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(conf, Seconds(3))

    println("Creating rabbit stream receiver..")
    val receiverStream = RabbitMQUtils.createStream[String](ssc, Map(
      "hosts"         -> "localhost",
      "queueName"     -> "simpl_spark_dev",
      "exchangeName"  -> "simpl_exchange",
      "exchangeType"  -> "fanout",
      "vHost"         -> "/",
      "userName"      -> "rootcss",
      "password"      -> "indian"
    ))
    receiverStream.start()

    println("Ready for processing..")
    receiverStream.foreachRDD(r => {
      if (!r.isEmpty()) {
        r.collect().foreach(println)
      } else {
        println("_Nothing arrived yet..")
      }
    })

    println("Initiating stream consumer..")
    ssc.start()
    ssc.awaitTermination()
  }

}
