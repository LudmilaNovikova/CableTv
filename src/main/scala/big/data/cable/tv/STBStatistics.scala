package big.data.cable.tv

import big.data.cable.tv.service.STBStatisticsFunctions
import big.data.cable.tv.service.STBStatisticsFunctions._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
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
      System.err.println(s"""
                            |Usage: STBStatistics <path to save statistics file>
            """.stripMargin)
      System.exit(1)
    }

    var timeStart = new DateTime()
/*
    logger.info("SELECT * FROM SbtStructuredMessage LIMIT 100")
    val dfAll = sqlContext.sql("SELECT * FROM SbtStructuredMessage LIMIT 100")

    STBStatisticsFunctions.writeCommonStatistics(dfAll, args(0))
    STBStatisticsFunctions.loggingDuration("periodCS:", timeStart, logger)
*/


    val columnStat  = Array("SbtStructuredMessage0.msgType","SbtStructuredMessage0.streamType","SbtStructuredMessage0.spyVersion","SbtStructuredMessage1.playerUrl")
    val countCluster = 4

    val dfInterval = sqlContext.sql("SELECT * FROM SbtStructuredMessage LIMIT 10")
    logger.info("SELECT * FROM SbtStructuredMessage LIMIT 10")

    println("dfInterval "+dfInterval.count())
    dfInterval.registerTempTable("Interval")
    sqlContext.sql("select sbtstructuredmessage0.mac from Interval").show()

    timeStart = new DateTime()

    val dfQ = STBStatisticsFunctions.initQTest(dfInterval,getDistinctMacQ(timeStart, countCluster), countCluster, timeStart)
    timeStart = STBStatisticsFunctions.loggingDuration("periodQ count" + dfQ.count() ,timeStart,logger)

    /*
    timeStart = new DateTime()
    val dfJ = STBStatisticsFunctions.initJ(sc, sqlContext, dfInterval, countCluster,columnStat)
    val periodJ = new Period(timeStart, new DateTime()).normalizedStandard()
    logger.info("periodJ:" + hms.print(periodJ) + " count" + dfJ.count())
*/
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
