package big.data.cable.tv

import big.data.cable.tv.model.User
import big.data.cable.tv.service.HiveService
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import big.data.cable.tv.STBStatistics._

/**
  * Created by Raslu on 23.03.2016.
  */
object STBStatisticsKMeans {

  val nameTableUsers = "STBKMeansUsers"
  val nameTableClusters = "STBKMeansClusters"

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Don't specify the count of cluster and iterations")
      System.exit(1)
    }

    val countCluster = args(0).toInt
    val countIterations = args(1).toInt

    val nameTableUsersNum = nameTableUsers + countCluster
    val nameTableClustersNum = nameTableClusters + countCluster

    val dfPrimariData = sqlContext.sql(
      """SELECT sbtstructuredmessage0.mac,cast(sbtstructuredmessage0.received as double), cast(sbtstructuredmessage0.linkFaults as double)
        |  ,cast(sbtstructuredmessage0.restored as double), cast(sbtstructuredmessage0.overflow as double)
        |  , cast(sbtstructuredmessage0.underflow as double), cast(sbtstructuredmessage1.uptime as double)
        |  , cast(sbtstructuredmessage1.vidDecodeErrors as double), cast(sbtstructuredmessage1.vidDataErrors as double)
        |  , cast(sbtstructuredmessage1.avTimeSkew as double), cast(sbtstructuredmessage1.avPeriodSkew as double)
        |  , cast(sbtstructuredmessage1.bufUnderruns as double), cast(sbtstructuredmessage1.bufOverruns as double)
        |  , cast(sbtstructuredmessage2.dvbLevel as double), cast(sbtstructuredmessage2.curBitrate as double)
        |  from SbtStructuredMessage LIMIT 10000000""".stripMargin)

    println("dfPrimariData - " + dfPrimariData.count())
    dfPrimariData.show()

    val vectors = dfPrimariData.map(s => createVectorFromRow(s))
    val clusters = KMeans.train(vectors, countCluster, countIterations)

    val usersRDD = dfPrimariData.map(s => User(s.getString(0),clusters.predict(createVectorFromRow(s))))

    import sqlContext.implicits._
    val usersDF = usersRDD.toDF;
    val usersGroupByDF = usersDF.groupBy(col("mac"), col("cluster")).count()
    usersGroupByDF.show()

    //insert into table Users
    HiveService.dropTable(nameTableUsersNum)
    HiveService.createTableUsers(nameTableUsersNum)
    HiveService.insertIntoTableDF(nameTableUsersNum, usersGroupByDF)
    //----------------------

    //insert into table Clusters
    HiveService.dropTable(nameTableClustersNum)
    HiveService.createTableClusters(nameTableClustersNum)
    var clusterNum = 0
    clusters.clusterCenters.foreach(cluster => {
      clusterNum = clusterNum + 1
      var centerNum = 0
      cluster.toArray.foreach(center => {
        centerNum = centerNum + 1
        println("INSERT INTO TABLE " + nameTableClustersNum + " select t.* from (select " + clusterNum + "," + centerNum + "," + center + ") t")
        sqlContext.sql("INSERT INTO TABLE " + nameTableClustersNum + " select t.* from (select " + clusterNum + "," + centerNum + "," + center + ") t")
      })
    })
    //--------------------------

    println("ClusterCenters: " + clusters.clusterCenters.to)
    val cost = clusters.computeCost(vectors)
    println("Sum of squared errors: " + cost)
    sc.stop()
  }


  def createVectorFromRow(r: org.apache.spark.sql.Row): Vector = {

    Vectors.dense (Array (r.getDouble(1), r.getDouble(2)
    , r.getDouble(3), r.getDouble(4)
    , r.getDouble(5), r.getDouble(6)
    , r.getDouble(7), r.getDouble(8)
    , r.getDouble(9), r.getDouble(10)
    , r.getDouble(11), r.getDouble(12)
    , r.getDouble(13), r.getDouble(14) ) )
  }
  def validation(d:Double):Double = {
    if(d==(-1)){
      return 0.toDouble
    } else{
      return d
    }

  }
}
