package big.data.cable.tv

import java.util.Properties

import big.data.cable.tv.STBStatistics._
import big.data.cable.tv.model.User
import big.data.cable.tv.service.{StbStructuredMessage, StbStructuredMessageService, HiveService}
import big.data.cable.tv.service.STBStatisticsFunctions._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{StreamingKMeans, KMeans}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time._

/**
  * Created by Raslu on 26.03.2016.
  */
object STBStreamingKMeans {
  val nameTableUsers = "STBKStreamingMeansUsers"
  val nameTableClusters = "STBStreamingKMeansClusters"

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println( s"""
                             |Usage: KafkaStreamProcessing <brokers> <Hive table name> <stb stream Kafka topic>
                             |  <brokers> is a list of one or more Kafka brokers
                             |  <Hive table name> table name for saving stb stream data
                             |  <stb stream kafka topic> Kafka topic used as stream data input for this spark streaming application
            """.stripMargin)
      System.exit(1)
    }

    val brokers = args(0)
    val topic = args(1)
    val countCluster = args(2).toInt
    val nameTableUsersNum = nameTableUsers + countCluster
    val nameTableClustersNum = nameTableClusters + countCluster


    //Инициализация контекстов
    val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaStreamProcessing")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new HiveContext(sc)


    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val kafkaParams = Map(
      "zookeeper.connect" -> "bigdata1.nnstu.com:2181",
      "group.id" -> "stbGroupId",
      "metadata.broker.list" -> brokers
    )

    //Create an input stream that directly pulls messages from Kafka Brokers
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //Create a model with random clusters and specify the number of clusters to find
    val numDimensions = 14
    var model = new StreamingKMeans().setK(countCluster).setDecayFactor(1.0).setRandomCenters(numDimensions, 0.0).latestModel()

    messages.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val valuesRdd: RDD[String] = rdd.map(x => x._2)
        val stbStructuredMessages = StbStructuredMessageService.getStbStructuredMessages(valuesRdd)

        //Perform a k-means update on a batch of data.
        model = model.update(cteateRDDVectors(stbStructuredMessages),0.8,"batches")

        //Determining to which cluster each point belongs from batch of data
        val usersRDD = stbStructuredMessages.map(s => User(s.stbStructuredMessage0.mac,model.predict(cteateVectors(s))))
        import sqlContext.implicits._
        val usersDF = usersRDD.toDF.groupBy(col("mac"), col("cluster")).count()

        //Insert into table Users
        HiveService.insertIntoTableDF(nameTableUsersNum, usersDF)

        //Insert into table Clusters
        //...
      }
    }
    )

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    sc.stop();
  }


  def cteateRDDVectors(rdd:RDD[StbStructuredMessage]):RDD[org.apache.spark.mllib.linalg.Vector] = {
    rdd.map(r => Vectors.dense (Array (r.stbStructuredMessage0.received.toDouble, r.stbStructuredMessage0.linkFaults.toDouble
      , r.stbStructuredMessage0.restored, r.stbStructuredMessage0.overflow.toDouble
      , r.stbStructuredMessage0.underflow.toDouble, r.stbStructuredMessage1.uptime.toDouble
      , r.stbStructuredMessage1.vidDecodeErrors.toDouble, r.stbStructuredMessage1.vidDataErrors.toDouble
      , r.stbStructuredMessage1.avTimeSkew.toDouble, r.stbStructuredMessage1.avPeriodSkew.toDouble
      , r.stbStructuredMessage1.bufUnderruns.toDouble, r.stbStructuredMessage1.bufOverruns.toDouble
      , r.stbStructuredMessage2.dvbLevel.toDouble, r.stbStructuredMessage2.curBitrate.toDouble ) ))


  }

  def cteateVectors(r: StbStructuredMessage):org.apache.spark.mllib.linalg.Vector = {
    Vectors.dense (Array (r.stbStructuredMessage0.received.toDouble, r.stbStructuredMessage0.linkFaults.toDouble
      , r.stbStructuredMessage0.restored, r.stbStructuredMessage0.overflow.toDouble
      , r.stbStructuredMessage0.underflow.toDouble, r.stbStructuredMessage1.uptime.toDouble
      , r.stbStructuredMessage1.vidDecodeErrors.toDouble, r.stbStructuredMessage1.vidDataErrors.toDouble
      , r.stbStructuredMessage1.avTimeSkew.toDouble, r.stbStructuredMessage1.avPeriodSkew.toDouble
      , r.stbStructuredMessage1.bufUnderruns.toDouble, r.stbStructuredMessage1.bufOverruns.toDouble
      , r.stbStructuredMessage2.dvbLevel.toDouble, r.stbStructuredMessage2.curBitrate.toDouble ) )

  }

}
