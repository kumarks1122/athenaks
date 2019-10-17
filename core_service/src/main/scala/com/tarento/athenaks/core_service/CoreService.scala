package com.tarento.athenaks.core_service

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object CoreService {
  def main(args: Array[String]): Unit = {
    print("Core services")
  }

  def putToStream(topic: String, value: String, key: String = null): Boolean = {
    try {
      val props = new Properties()
      props.put("bootstrap.servers", "192.168.56.102:9094")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      val record = new ProducerRecord[String, String](topic, value)
      producer.send(record)
      producer.close()

      return true
    } catch {
      case ex: Exception =>{
        return false
      }
    }

  }
}
