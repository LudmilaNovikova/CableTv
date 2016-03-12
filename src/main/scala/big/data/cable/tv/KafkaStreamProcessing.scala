package big.data.cable.tv

import java.util.Properties

import big.data.cable.tv.service.{SbtStructuredMessageService, HiveService}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by lnovikova on 30.01.2016.
 */
object KafkaStreamProcessing {

  def main(args: Array[String]): Unit = {

    /*
        if (args.length < 1) {
          System.err.println(s"""
            |Usage: KafkaStreamProcessing <brokers>
            |  <brokers> is a list of one or more Kafka brokers
            |
            """.stripMargin)
          System.exit(1)
        }
    */
    val brokers = "bigdata1.nnstu.com:9092" //args(0)

    val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaStreamProcessing")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val sqlContext = new HiveContext(sc)

val props: Properties = new Properties
    props.put("bootstrap.servers", "192.168.1.31:9092")
//    props.put("zk.connect", args(1))
    props.put("acks", "0")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val TOPIC: String = "SbtFailure"
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String](TOPIC, "test again"))

    HiveService.createTableSbtStructuredMessage(sqlContext)

    // Create direct kafka stream with brokers and topics
    val topic = "SbtStream"
    val topicsSet = Set(topic)
    val kafkaParams = Map(
      "zookeeper.connect" -> "bigdata1.nnstu.com:2181",
      "group.id" -> "sbtGroupId",
      "metadata.broker.list" -> brokers
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    messages.foreachRDD(rdd => {
      if (rdd.count() > 0) {
//        println("Going to save to file: " + rdd.collect().foreach(println(_)))
        // save to hdfs
//        rdd.saveAsTextFile("cableTvDataRdd")
        // save to Hive
        val valuesRdd: RDD[String] = rdd.map(x => x._2)
        val sbtStructuredMessages = SbtStructuredMessageService.getSbtStructuredMessages(valuesRdd)
        HiveService.insertIntoTable(sqlContext, "SbtStructuredMessage", sbtStructuredMessages)
/*        rdd.foreach(record =>
          println(record) // executed at the worker
        )*/
      }
    }
    )
//    messages.print(5)
    //    messages.saveAsTextFiles("cableTvData", "txt")
    //    messages.saveAsHadoopFiles("cableTvData", "txt")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    sc.stop();

  }

}
