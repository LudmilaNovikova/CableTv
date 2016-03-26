package big.data.cable.tv

import big.data.cable.tv.service.{HiveService, STBStatisticsFunctions}
import big.data.cable.tv.service.STBStatisticsFunctions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{Period, DateTime}
import org.joda.time.format.PeriodFormatterBuilder

/**
  * Created by Raslu on 22.02.2016.
  */
object STBStatistics {

  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sc)
  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit ={
    if (args.length < 1) {
      System.err.println("""
                            |Usage: STBStatistics <path to save statistics file>
            """.stripMargin)
      System.exit(1)
    }

    var timeStart = new DateTime()
/*
    STBStatisticsFunctions.printlnCommonStatistics()
    printlnDuration("periodCS:", timeStart)
*/


    val columnStat  = Array("SbtStructuredMessage0.msgType","SbtStructuredMessage0.streamType")
    val countCluster = 4

    timeStart = new DateTime()

    //val logger = Logger.getLogger(getClass.getName)
    //HiveService.creataTableStbQ(sqlContext,countCluster)

    timeStart = printlnDuration("Creating Hive table Q ", timeStart)

    //INSERT INTO TABLE primaryData
    HiveService.dropTable("primaryData")
    val dfPrimaryData = HiveService.createTablePrimaryData()

    val dfQ = STBStatisticsFunctions.initQTest2_3(countCluster, timeStart)
    timeStart = printlnDuration("periodQ count" + dfQ.count() ,timeStart)

    val dfJ = STBStatisticsFunctions.initJTest_2_3(countCluster,columnStat, timeStart)
    timeStart = printlnDuration("periodJ count" + dfJ.count() ,timeStart)

    /*

        timeStart = new DateTime()
        val dfH = STBStatisticsFunctions.H(sqlContext,dfQ,dfJ)
        val periodH = new Period(timeStart, new DateTime()).normalizedStandard()
        logger.info("periodH:" + hms.print(periodH)+" count" +dfH.count())


        timeStart = new DateTime()
        val dfN = STBStatisticsFunctions.initN(sc, sqlContext, dfInterval, columnStat)
        val periodN = new Period(timeStart, new DateTime()).normalizedStandard()
        logger.info("periodN:" + hms.print(periodN)+" count" +dfN.count())
    */

    STBStatisticsFunctions.loggingDuration("sc.stop()" ,timeStart,logger)
    sc.stop()

  }

  def getDistinctMacQ(timeSt: DateTime, countCluster: Int): DataFrame={
    val createQ = sqlContext.sql("CREATE TABLE IF NOT EXISTS Q (" +
      "mac String," +
      "cluster Int," +
      "pvod Double" +
      ")")
    var timeStart = loggingDuration("Creating Hive table Q - " + createQ.count(), timeSt, logger)

    //checking the count of clusters
    val dfCluster = sqlContext.sql("select distinct cluster from Q")
    if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
      logger.info("DELETE FROM Q")
      sqlContext.sql("DELETE FROM Q")
    }
    timeStart = loggingDuration("checking the count of clusters " + dfCluster.count() + "(" + countCluster + ")", timeStart, logger)

    val dfMac = sqlContext.sql("select distinct mac from Q")
    return dfMac;
  }
}
