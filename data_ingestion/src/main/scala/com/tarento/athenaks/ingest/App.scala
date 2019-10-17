package com.tarento.athenaks.ingest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.tarento.athenaks.core_service.CoreService
import com.tarento.athenaks.core_service.EventDeserializer
import com.google.gson.JsonObject

object App{
  def main(args: Array[String]): Unit = {
    println("pipeline started")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.56.102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[EventDeserializer],
      "group.id" -> "ingestion-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("rftestmain")

    val conf = new SparkConf().setMaster("local[4]").setAppName("Ingest")
    val ssc = new StreamingContext(conf, Seconds(10))

    val stream = KafkaUtils.createDirectStream[String, JsonObject](
      ssc,
      PreferConsistent,
      Subscribe[String, JsonObject](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        process(record)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


  def process(record: ConsumerRecord[String, JsonObject]): Unit = {
    println(record.value().get("range_no"))
  }
}
