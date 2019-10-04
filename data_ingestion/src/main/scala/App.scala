import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object App {
  def main(args: Array[String]): Unit = {
    println("pipeline started")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.56.102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ingestion-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("rftestmain")

    val conf = new SparkConf().setMaster("local[4]").setAppName("Ingest")
    val ssc = new StreamingContext(conf, Seconds(10))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record=>(record.value().toString)).print

    ssc.start()
    ssc.awaitTermination()
  }
}
