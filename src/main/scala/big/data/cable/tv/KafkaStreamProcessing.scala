package big.data.cable.tv

import java.util.Properties

import big.data.cable.tv.service.{StbStructuredMessage, StbStructuredMessageService, HiveService}
import kafka.producer.Producer
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

  val topic = "StbStream"
  val failureTopic: String = "StbFailure"
  var producer: KafkaProducer[String, String] = null

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println( s"""
            |Usage: KafkaStreamProcessing <brokers> <Hive table name>
            |  <brokers> is a list of one or more Kafka brokers
            |  <Hive table name> table name for saving stb stream data
            """.stripMargin)
      System.exit(1)
    }
    initProducer()

    val brokers = args(0)
    val stbDataTableName = args(1)

    val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaStreamProcessing")
    //    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val sqlContext = new HiveContext(sc)

    HiveService.createTableStbStructuredMessage(sqlContext, stbDataTableName)

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val kafkaParams = Map(
      "zookeeper.connect" -> "bigdata1.nnstu.com:2181",
      "group.id" -> "stbGroupId",
      "metadata.broker.list" -> brokers
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    messages.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val valuesRdd: RDD[String] = rdd.map(x => x._2)
        sendToKafkaFailureTopic(valuesRdd)
        val stbStructuredMessages = StbStructuredMessageService.getStbStructuredMessages(valuesRdd)
        HiveService.insertIntoTable(sqlContext, stbDataTableName, stbStructuredMessages)
      }
    }
    )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    sc.stop();

  }

  def sendToKafkaFailureTopic(messages: RDD[String]): Long = {
    val filteredMessages = messages.filter(r => {
      (r.split(" "))(44).toInt > 0
    })
    val count = filteredMessages.count()
    println("Found messages messages with buffer underruns > 0. Count: " + count)
    filteredMessages.collect().foreach(r => {
      producer.send(new ProducerRecord[String, String](failureTopic, r))
    }
    )
    count
  }

  def initProducer(): Unit = {
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

    producer = new KafkaProducer[String, String](props)
  }

}
