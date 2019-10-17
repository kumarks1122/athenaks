package com.tarento.athenaks.core_service

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util._
import com.google.gson.{Gson, JsonObject}
import org.apache.kafka.common.serialization.Deserializer


class EventDeserializer extends Deserializer[JsonObject]{
  override def deserialize(topic:String, jsonBytes: Array[Byte]) = {
    val jsonString = (jsonBytes.map(_.toChar)).mkString
    val gson = new Gson
    val event = gson.fromJson(jsonString, classOf[JsonObject])
    event

//    val byteIn = new ByteArrayInputStream(jsonBytes)
//    val objIn = new ObjectInputStream(byteIn)
//    val obj = objIn.readObject().asInstanceOf[Event]
//    byteIn.close()
//    objIn.close()
//    obj
  }
  override def close():Unit = {

  }

  override def configure(configs: Map[String, _], isKey: Boolean): Unit = {

  }

}