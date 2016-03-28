package big.data.cable.tv

import java.util.Properties

import big.data.cable.tv.model.User
import big.data.cable.tv.service.{StbStructuredMessage, HiveService, StbStructuredMessageService}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import big.data.cable.tv.STBStatistics._
import big.data.cable.tv.service.STBStatisticsFunctions._
import org.joda.time.DateTime

/**
 * Created by lnovikova on 30.01.2016.
 */
object KafkaStreamingKMeans {

  val failureTopic: String = "StbFailure"
  var producer: KafkaProducer[String, String] = null
  val nameTableUsers = "STBStreamingMeansUsers"
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
    initProducer()

    val brokers = args(0)
    val topic = args(1)
    val countCluster = args(2).toInt
    val nameTableUsersNum = nameTableUsers + countCluster
    val nameTableClustersNum = nameTableClusters + countCluster


    //val sparkConf = new SparkConf()
    sparkConf.setAppName("KafkaStreamProcessing")
    //val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    //val sqlContext = new HiveContext(sc)

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val kafkaParams = Map(
      "zookeeper.connect" -> "bigdata1.nnstu.com:2181",
      "group.id" -> "stbGroupId",
      "metadata.broker.list" -> brokers
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //StreamingKMeans
    val numDimensions = 14
    println("new StreamingKMeans()")
    var timeStart = printlnDuration("new StreamingKMeans()", new DateTime())
    var model = new StreamingKMeans()
      .setK(countCluster)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0).latestModel()
    timeStart = printlnDuration("new StreamingKMeans()-END ", timeStart)
    //----------------

    HiveService.dropTable(nameTableUsersNum)
    HiveService.createTableUsers(nameTableUsersNum)


    messages.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val timeDuration = new DateTime()
        val valuesRdd: RDD[String] = rdd.map(x => x._2)
        val stbStructuredMessages = StbStructuredMessageService.getStbStructuredMessages(valuesRdd)

        //Perform a k-means update on a batch of data.
        timeStart = printlnDuration("StreamingKMeans() update", timeStart)
        model = model.update(cteateRDDVectors(stbStructuredMessages),0.8,"batches")
        timeStart = printlnDuration("StreamingKMeans() update - END", timeStart)

        val usersRDD = stbStructuredMessages.map(s => User(s.stbStructuredMessage0.mac,model.predict(cteateVectors(s))))
        import sqlContext.implicits._
        val usersDF = usersRDD.toDF.groupBy(col("mac"), col("cluster")).count()
        timeStart = printlnDuration("create usersDF count  "+usersDF.count(), timeStart)
        usersDF.show()

        /*
        //create table Users
        HiveService.createTableUsers(nameTableUsersNum)
        //----------------------
        val usersAllDF = sqlContext.sql("select * from "+nameTableUsersNum)
        timeStart = printlnDuration("select * from "+nameTableUsersNum+"; count "+usersAllDF.count(), timeStart)
        usersAllDF.show()

        val newUsersAllDF = usersAllDF.unionAll(usersDF)
        timeStart = printlnDuration("union all newUsersAllDF, count "+newUsersAllDF.count(), timeStart)
        newUsersAllDF.show()

        println("group by newUsersAllDF")
        val usersGroupByDF = newUsersAllDF.groupBy(col("mac"), col("cluster")).agg(sum("count").as("count"))
        timeStart = printlnDuration("group by newUsersAllDF, count "+usersGroupByDF.count(), timeStart)
        usersGroupByDF.show()

        //insert into table Users
        HiveService.dropTable(nameTableUsersNum)
        HiveService.createTableUsers(nameTableUsersNum)
        HiveService.insertIntoTableDF(nameTableUsersNum, usersGroupByDF)
        //----------------------
        */

        HiveService.insertIntoTableDF(nameTableUsersNum, usersDF)

        //checking count rows
        val checkCount = sqlContext.sql("select count(*) from "+nameTableUsersNum).first().getLong(0)
        timeStart = printlnDuration("select count(*) from "+nameTableUsersNum+"; count - "+checkCount, timeStart)


        //insert into table Clusters
        HiveService.dropTable(nameTableClustersNum)
        HiveService.createTableClusters(nameTableClustersNum)
        var clusterNum = 0
        model.clusterCenters.foreach(cluster => {
          clusterNum = clusterNum + 1
          var centerNum = 0
          cluster.toArray.foreach(center => {
            centerNum = centerNum + 1
            //println("INSERT INTO TABLE " + nameTableClustersNum + " select t.* from (select " + clusterNum + "," + centerNum + "," + center + ") t")
            sqlContext.sql("INSERT INTO TABLE " + nameTableClustersNum + " select t.* from (select " + clusterNum + "," + centerNum + "," + center + ") t")
          })
        })
        //--------------------------
        printlnDuration("DURATION",timeDuration)

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
