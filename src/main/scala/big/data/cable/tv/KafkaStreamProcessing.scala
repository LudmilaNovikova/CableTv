package big.data.cable.tv

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
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
//    val brokers = "sandbox.hortonworks.com:6667"//args(0)
    val brokers = "bigdata1.nnstu.com:9092"//args(0)

    val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaStreamProcessing")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topic = "SbtStream"
    val topicsSet = Set(topic)
    val kafkaParams = Map(
      "zookeeper.connect" -> "bigdata1.nnstu.com:2181",
      "zookeeper.connection.timeout.ms" -> "1000",
      "metadata.broker.list" -> brokers
    )
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream(ssc, kafkaParams, topicsSet)
    messages.print(5)
//    directKafkaStream.foreachRDD(rdd => for (line <- rdd) println(line))
//    val messages2 = messages.map(_._2)

//    messages.saveAsTextFiles("cableTvData", "txt")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

    sc.stop();

  }

}
