package big.data.cable.tv

import big.data.cable.tv.service.STBStatisticsFunctions
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{Period, DateTime}
import org.joda.time.format.PeriodFormatterBuilder

/**
  * Created by Raslu on 22.02.2016.
  */
object STBStatistics {

  def main(args: Array[String]): Unit ={
    val logger = Logger.getLogger(getClass.getName)
    logger.info("i write logs!!!!!!")
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: STBStatistics <path to save statistics file>
            """.stripMargin)
      System.exit(1)
    }

    logger.info("create context")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("STBStatistics")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    logger.info("create context success")


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
    dfInterval.show()

    timeStart = new DateTime()
    val dfQ = STBStatisticsFunctions.initQ(sc, sqlContext, dfInterval, countCluster,timeStart)
    STBStatisticsFunctions.loggingDuration("periodQ count" + dfQ.count()+":",timeStart,logger)

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

    sc.stop()
  }
}
