package big.data.cable.tv.service


import big.data.cable.tv.STBStatistics._
import org.joda.time.{Period, DateTime}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}
import org.joda.time.format.PeriodFormatterBuilder
import org.slf4j.LoggerFactory


/**
  * Created by Raslu on 13.02.2016.
  */
object STBStatisticsFunctions {
  def writeCommonStatistics(primaryStatDF: DataFrame, pathStatistic: String): Unit = {
    import java.io._
    val writer = new PrintWriter(new File(pathStatistic + "/statistics.txt"))
    writer.write(PrintDF.showString(primaryStatDF.describe("StbStructuredMessage0.counter", "StbStructuredMessage0.received", "StbStructuredMessage0.linkFaults", "StbStructuredMessage0.restored"
      , "StbStructuredMessage0.overflow", "StbStructuredMessage0.underflow", "StbStructuredMessage1.uptime", "StbStructuredMessage1.vidDecodeErrors", "StbStructuredMessage1.vidDataErrors"
      , "StbStructuredMessage1.avTimeSkew", "StbStructuredMessage1.avPeriodSkew", "StbStructuredMessage1.bufUnderruns", "StbStructuredMessage1.bufOverruns", "StbStructuredMessage2.dvbLevel"
      , "StbStructuredMessage2.curBitrate")
    )
    )

    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage0.lost>=0").describe("StbStructuredMessage0.lost")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage0.mdiDf>=0").describe("StbStructuredMessage0.mdiDf")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage0.mdiMlr>=0").describe("StbStructuredMessage0.mdiMlr")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage0.regionId>=0").describe("StbStructuredMessage0.regionId")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.contentType>=0").describe("StbStructuredMessage1.contentType")))

    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.transportOuter>=0").describe("StbStructuredMessage1.transportOuter")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.transportInner>=0").describe("StbStructuredMessage1.transportInner")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.channelId>=0").describe("StbStructuredMessage1.channelId")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.playSession>=0").describe("StbStructuredMessage1.playSession")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.scrambled>=0").describe("StbStructuredMessage1.scrambled")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.powerState>=0").describe("StbStructuredMessage1.powerState")))
    //в инструкции написано 0 - неизвестно. Но в выборке присутствуют только значения -1(3) и 0 (30003)
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.casType>0").describe("StbStructuredMessage1.casType"))) //primaryStatDF.groupBy("casType").count().show()
    //36 CAS_KEY_TIME  0 - неизвестно
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.casKeyTime>0").describe("StbStructuredMessage1.casKeyTime"))) //primaryStatDF.groupBy("casKeyTime").count().show()
    //37 VID_FRAMES 0 - видеостати стика недоступна
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.vidFrames>0").describe("StbStructuredMessage1.vidFrames")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.audFrames>0").describe("StbStructuredMessage1.audFrames")))
    //т.к. поле audDataErrors показывает наличие ошибок при даступной аудиостатискики (audFrames>0) 0 здесь тоже информация для записей которые audFrames>0
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage1.audFrames>0").describe("StbStructuredMessage1.audDataErrors")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage2.sdpObjectId>=0").describe("StbStructuredMessage2.sdpObjectId")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage2.dvbLevelGood>=0").describe("StbStructuredMessage2.dvbLevelGood")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage2.dvbLevel>=0").describe("StbStructuredMessage2.dvbLevel")))
    writer.write(PrintDF.showString(primaryStatDF.filter("StbStructuredMessage2.dvbFrequency>=0").describe("StbStructuredMessage2.dvbFrequency")))



    writer.write(PrintDF.showString(primaryStatDF.groupBy("StbStructuredMessage0.msgType").count()))
    writer.write(PrintDF.showString(primaryStatDF.groupBy("StbStructuredMessage0.streamType").count()))
    //primaryStatDF.groupBy("mac").count().show()
    //primaryStatDF.groupBy("streamAddr").count().show()
    writer.write(PrintDF.showString(primaryStatDF.groupBy("StbStructuredMessage0.lostOverflow").count()))
    writer.write(PrintDF.showString(primaryStatDF.groupBy("StbStructuredMessage0.plc").count()))
    //serviceAccountNumber
    //val dfFilterServiceAccountNumber = primaryStatDF.filter("serviceAccountNumber not in ('-1','N/A')")
    //dfFilterServiceAccountNumber.groupBy("serviceAccountNumber").count().join(dfFilterServiceAccountNumber.agg(count("serviceAccountNumber").as("countAll"))).show
    //primaryStatDF.groupBy("stbIp").count().show()
    writer.write(PrintDF.showString(primaryStatDF.groupBy("StbStructuredMessage0.spyVersion").count()))
    //playerUrl
    val dfFilterPlayerUrl = primaryStatDF.filter("StbStructuredMessage1.playerUrl not in ('X')")
    writer.write(PrintDF.showString(dfFilterPlayerUrl.groupBy("StbStructuredMessage1.playerUrl").count().join(dfFilterPlayerUrl.agg(count("StbStructuredMessage1.playerUrl").as("countAll")))))

    writer.close()

    def logger = LoggerFactory.getLogger(this.getClass)
    logger.info("select data about 5 users")

    val macListDF = primaryStatDF.groupBy(col("StbStructuredMessage0.mac").as("mac1")).count().orderBy(desc("count")).limit(10).select("mac1")
    val macDF = primaryStatDF.join(macListDF, macListDF("mac1") === primaryStatDF("mac")).select(primaryStatDF.col("*")) //.select(primaryStatDF.columns.mkString(", "))
    //macDF.show
    //macDF.write.parquet("parquetTest")
  }

  def printlnCommonStatistics(): Unit = {
    val primaryStatDF = sqlContext.sql("SELECT * FROM SbtStructuredMessage")
    import java.io._
    primaryStatDF.describe("sbtstructuredmessage0.counter", "sbtstructuredmessage0.received", "sbtstructuredmessage0.linkFaults", "sbtstructuredmessage0.restored"
      , "sbtstructuredmessage0.overflow", "sbtstructuredmessage0.underflow", "sbtstructuredmessage1.uptime", "sbtstructuredmessage1.vidDecodeErrors", "sbtstructuredmessage1.vidDataErrors"
      , "sbtstructuredmessage1.avTimeSkew", "sbtstructuredmessage1.avPeriodSkew", "sbtstructuredmessage1.bufUnderruns", "sbtstructuredmessage1.bufOverruns", "sbtstructuredmessage2.dvbLevel"
      , "sbtstructuredmessage2.curBitrate").show()



    primaryStatDF.filter("sbtstructuredmessage0.lost>=0").describe("sbtstructuredmessage0.lost").show()
    primaryStatDF.filter("sbtstructuredmessage0.mdiDf>=0").describe("sbtstructuredmessage0.mdiDf").show()
    primaryStatDF.filter("sbtstructuredmessage0.mdiMlr>=0").describe("sbtstructuredmessage0.mdiMlr").show()
    primaryStatDF.filter("sbtstructuredmessage0.regionId>=0").describe("sbtstructuredmessage0.regionId").show()
    primaryStatDF.filter("sbtstructuredmessage1.contentType>=0").describe("sbtstructuredmessage1.contentType").show()

    primaryStatDF.filter("sbtstructuredmessage1.transportOuter>=0").describe("sbtstructuredmessage1.transportOuter").show()
    primaryStatDF.filter("sbtstructuredmessage1.transportInner>=0").describe("sbtstructuredmessage1.transportInner").show()
    primaryStatDF.filter("sbtstructuredmessage1.channelId>=0").describe("sbtstructuredmessage1.channelId").show()
    primaryStatDF.filter("sbtstructuredmessage1.playSession>=0").describe("sbtstructuredmessage1.playSession").show()
    primaryStatDF.filter("sbtstructuredmessage1.scrambled>=0").describe("sbtstructuredmessage1.scrambled").show()
    primaryStatDF.filter("sbtstructuredmessage1.powerState>=0").describe("sbtstructuredmessage1.powerState").show()
    //в инструкции написано 0 - неизвестно. Но в выборке присутствуют только значения -1(3) и 0 (30003)
    primaryStatDF.filter("sbtstructuredmessage1.casType>0").describe("sbtstructuredmessage1.casType").show() //primaryStatDF.groupBy("casType").count().show()
    //36 CAS_KEY_TIME  0 - неизвестно
    primaryStatDF.filter("sbtstructuredmessage1.casKeyTime>0").describe("sbtstructuredmessage1.casKeyTime").show() //primaryStatDF.groupBy("casKeyTime").count().show()
    //37 VID_FRAMES 0 - видеостати стика недоступна
    primaryStatDF.filter("sbtstructuredmessage1.vidFrames>0").describe("sbtstructuredmessage1.vidFrames").show()
    primaryStatDF.filter("sbtstructuredmessage1.audFrames>0").describe("sbtstructuredmessage1.audFrames").show()
    //т.к. поле audDataErrors показывает наличие ошибок при даступной аудиостатискики (audFrames>0) 0 здесь тоже информация для записей которые audFrames>0
    primaryStatDF.filter("sbtstructuredmessage1.audFrames>0").describe("sbtstructuredmessage1.audDataErrors").show()
    primaryStatDF.filter("sbtstructuredmessage2.sdpObjectId>=0").describe("sbtstructuredmessage2.sdpObjectId").show()
    primaryStatDF.filter("sbtstructuredmessage2.dvbLevelGood>=0").describe("sbtstructuredmessage2.dvbLevelGood").show()
    primaryStatDF.filter("sbtstructuredmessage2.dvbLevel>=0").describe("sbtstructuredmessage2.dvbLevel").show()
    primaryStatDF.filter("sbtstructuredmessage2.dvbFrequency>=0").describe("sbtstructuredmessage2.dvbFrequency").show()



    primaryStatDF.groupBy("StbStructuredMessage0.msgType").count().show()
    primaryStatDF.groupBy("StbStructuredMessage0.streamType").count().show()
    //primaryStatDF.groupBy("mac").count().show()
    //primaryStatDF.groupBy("streamAddr").count().show()
    primaryStatDF.groupBy("StbStructuredMessage0.lostOverflow").count().show()
    primaryStatDF.groupBy("StbStructuredMessage0.plc").count().show()
    //serviceAccountNumber
    //val dfFilterServiceAccountNumber = primaryStatDF.filter("serviceAccountNumber not in ('-1','N/A')")
    //dfFilterServiceAccountNumber.groupBy("serviceAccountNumber").count().join(dfFilterServiceAccountNumber.agg(count("serviceAccountNumber").as("countAll"))).show
    //primaryStatDF.groupBy("stbIp").count().show()
    primaryStatDF.groupBy("StbStructuredMessage0.spyVersion").count().show()
    //playerUrl
    val dfFilterPlayerUrl = primaryStatDF.filter("StbStructuredMessage1.playerUrl not in ('X')")
    dfFilterPlayerUrl.groupBy("StbStructuredMessage1.playerUrl").count().join(dfFilterPlayerUrl.agg(count("StbStructuredMessage1.playerUrl").as("countAll"))).show()

    val macListDF = primaryStatDF.groupBy(col("StbStructuredMessage0.mac").as("mac1")).count().orderBy(desc("count")).limit(10).select("mac1")
    val macDF = primaryStatDF.join(macListDF, macListDF("mac1") === primaryStatDF("mac")).select(primaryStatDF.col("*")) //.select(primaryStatDF.columns.mkString(", "))
    macDF.show
    //macDF.write.parquet("parquetTest")
  }

  def initN(sc: SparkContext, sqlContext: SQLContext, primaryStatDF: DataFrame, columnStat: Array[String]): DataFrame = {

    //create dfN
    val schemaN = StructType(
      StructField("mac", StringType, false) ::
        StructField("value", StringType, false) ::
        StructField("count", IntegerType, false) ::
        StructField("columnName", StringType, false) :: Nil)
    var dfN = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaN)

    for (i <- 0 to columnStat.size - 1) {
      val value = primaryStatDF.groupBy(col("mac"), col(columnStat(i)).as("value")).count().withColumn("columnName", lit(columnStat(i): String).cast(StringType))
      dfN = dfN.unionAll(value)
    }
    ///dfN.show(100)
    //end --create dfN
    return dfN
  }

  def initQ(dfPrimaryData: DataFrame, countCluster: Int, timeSt: DateTime): DataFrame = {

      val logger = Logger.getLogger(getClass.getName)
      val createQ = sqlContext.sql("CREATE TABLE IF NOT EXISTS Q (" +
        "mac String," +
        "cluster Int," +
        "pvod Double" +
        ")")

      //checking the count of clusters
      val dfCluster = sqlContext.sql("select distinct cluster from Q")
      if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
        logger.info("DELETE FROM Q")
        sqlContext.sql("DELETE FROM Q")
      }

    dfPrimaryData.registerTempTable("PrimaryData")
    val dfPrimaryDataDistMac = sqlContext.sql("SELECT DISTINCT(stbStructuredMessage0.mac) as macDist from PrimaryData")
    println("dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count())
    dfPrimaryDataDistMac.show()
    dfPrimaryDataDistMac.registerTempTable("primaryDataDistMac")

    val dfMac = sqlContext.sql("select distinct mac from Q")
    dfMac.registerTempTable("distMac")
    //timeStart = loggingDuration("register temp table dfMac " + dfMac.count(), timeStart, logger)

    val dfActualDistMac = sqlContext.sql(
      """SELECT pd.macDist FROM primaryDataDistMac pd
        |left join distMac dm on (pd.macDist = dm.mac)
        |WHERE  dm.mac is null
      """.stripMargin)
    //timeStart = loggingDuration("select actual mac: DF dfActualDistMac " + dfActualDistMac.count() , timeStart, logger)

    dfActualDistMac.registerTempTable("actualDistMac")
    println("dfActualDistMac " + dfActualDistMac.count())
    dfActualDistMac.show()

    val schemaQ = StructType(
      StructField("mac", StringType, false) ::
        StructField("cluster", IntegerType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfQ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaQ)
    dfQ.registerTempTable("Q")

    if (dfActualDistMac.count() != 0) {
      //timeStart = loggingDuration("count dfActualDistMac: " + dfActualDistMac.count() , timeStart, logger)
      for (i <- 1 to countCluster) {
        val valueSum = dfActualDistMac
          .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
          .groupBy(col("macDist")).agg(sum("pvod").as("sum_pvod"))
          .withColumn("cluster", lit(i: Int).cast(IntegerType))
          .withColumn("rand", rand().cast(DoubleType))
          .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
          .select(col("macDist").as("mac"), col("cluster"), col("pvod"))
        //timeStart = loggingDuration("for i" + i + " valueSum: " + valueSum.count() , timeStart, logger)
        valueSum.show(100)
        dfQ = dfQ.unionAll(valueSum)
        //timeStart = loggingDuration("for i" + i + " dfQ.unionAll(valueSum): " + dfQ.count() , timeStart, logger)
      }

      dfQ.show(100)
      /*
      val checksumDF = dfQ.groupBy(col("mac")).agg(sum("pvod").as("checksum")).filter("checksum<>1")
      checksumDF.show(100)
      val checksumCount = checksumDF.count()
      if (checksumCount != 0) throw new Exception("checksumQCount != 1")
      timeStart = loggingDuration("checksumCount - "+checksumCount , timeStart, logger)

      dfQ.registerTempTable("dfQ")
      sqlContext.sql("INSERT INTO TABLE Q SELECT mac,cluster,pvod FROM dfQ")
      timeStart = loggingDuration("INSERT INTO TABLE Q - " + dfQ.count() , timeStart, logger)
*/
    }

    //end --create dfQ
    //dfQ.show(100)
    //timeStart = loggingDuration("return dfQ - " + dfQ.count() , timeStart, logger)
    return dfQ
  }

  def initQTest(dfPrimaryData: DataFrame, dfMac: DataFrame, countCluster: Int, timeSt: DateTime): DataFrame = {
    println("dfPrimaryData " + dfPrimaryData.count())
    dfPrimaryData.show()

    val dfPrimaryDataDistMac = dfPrimaryData.select("stbStructuredMessage0.mac").as("macDist").distinct()
    println("dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count())
    dfPrimaryDataDistMac.show()

    val dfActualDistMac = dfPrimaryDataDistMac.join(dfMac, dfPrimaryDataDistMac("macDist") === dfMac("mac"), "left_outer").where("mac is null")

    println("dfActualDistMac " + dfActualDistMac.count())
    dfActualDistMac.show()

    var dfQ = dfActualDistMac.select("macDist").as("mac").withColumn("cluster", lit(1: Int).cast(IntegerType))
      .withColumn("pvod", rand().cast(DoubleType))

    if (dfActualDistMac.count() != 0) {
      for (i <- 2 to countCluster) {
        val valueSum = dfActualDistMac
          .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
          .groupBy(col("macDist")).agg(sum("pvod").as("sum_pvod"))
          .withColumn("cluster", lit(i: Int).cast(IntegerType))
          .withColumn("rand", rand().cast(DoubleType))
          .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
          .select(col("macDist").as("mac"), col("cluster"), col("pvod"))
        valueSum.show(100)
        dfQ = dfQ.unionAll(valueSum)
        //timeStart = loggingDuration("for i" + i + " dfQ.unionAll(valueSum): " + dfQ.count() , timeStart, logger)
      }
      dfQ.show(100)


      val checksumDF = dfQ.groupBy(col("mac")).agg(sum("pvod").as("checksum")).filter("checksum<>1")
      checksumDF.show(100)
      val checksumCount = checksumDF.count()
      if (checksumCount != 0) throw new Exception("checksumQCount != 1")
    }


    //end --create dfQ
    //dfQ.show(100)
    //timeStart = loggingDuration("return dfQ - " + dfQ.count() , timeStart, logger)
    return dfQ
  }

  def initQTest2(countCluster: Int, timeSt: DateTime): DataFrame = {

    HiveService.dropTable("initQPrimaryData")
    HiveService.createTableStbStructuredMessage(sqlContext,"initQPrimaryData")

    sqlContext.sql("INSERT INTO TABLE initQPrimaryData SELECT * FROM SbtStructuredMessage LIMIT 50")

    val logger = Logger.getLogger(getClass.getName)
    val createQ = sqlContext.sql("CREATE TABLE IF NOT EXISTS Q (" +
      "mac String," +
      "cluster Int," +
      "pvod Double" +
      ")")
    var timeStart = printlnDuration("Creating Hive table Q - " + createQ.count(), timeSt)

    //checking the count of clusters
    val dfCluster = sqlContext.sql("select distinct cluster from Q")
    if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
      logger.info("DELETE FROM Q")
      sqlContext.sql("DELETE FROM Q")
    }
    timeStart = printlnDuration("checking the count of clusters " + dfCluster.count() + "(" + countCluster + ")", timeStart)

    val dfPrimaryDataDistMac = sqlContext.sql("SELECT DISTINCT(stbStructuredMessage0.mac) as macDist from initQPrimaryData")
    println("dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count())
    dfPrimaryDataDistMac.show()
    dfPrimaryDataDistMac.registerTempTable("primaryDataDistMac")
    timeStart = printlnDuration("register temp table dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count(), timeStart)

    val dfMac = sqlContext.sql("select distinct mac from Q")
    dfMac.registerTempTable("distMac")
    timeStart = printlnDuration("register temp table dfMac " + dfMac.count(), timeStart)

    val dfActualDistMac = sqlContext.sql(
      """SELECT pd.macDist FROM primaryDataDistMac pd
        |left join distMac dm on (pd.macDist = dm.mac)
        |WHERE  dm.mac is null
      """.stripMargin)
    timeStart = printlnDuration("select actual mac: DF dfActualDistMac " + dfActualDistMac.count(), timeStart)

    dfActualDistMac.registerTempTable("actualDistMac")
    println("dfActualDistMac " + dfActualDistMac.count())
    dfActualDistMac.show()

//    timeStart = printlnDuration("count dfActualDistMac: " + dfActualDistMac.count() , timeStart)
//    var dfQ = sqlContext.sql("select macDist as mac,cast('1' as INT) ,cast(rand() as decimal(38,37))  as pvod from actualDistMac")
//    timeStart = printlnDuration(" dfQ " + dfQ.count() , timeStart)
//    dfQ.show()
//    dfQ.registerTempTable("dfQ")

    val schemaQ = StructType(
    StructField("mac", StringType, false) ::
    StructField("cluster", IntegerType, false) ::
    StructField("pvod", DecimalType(11,10), false) :: Nil)
    var dfQ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaQ)

//    sqlContext.udf.register("udfTest",(c: java.math.BigDecimal, r: java.math.BigDecimal, isLastClaster: Boolean) =>
//      {
//        val one = new java.math.BigDecimal(1)
//        val pvod = one.subtract(c)
//        if (isLastClaster) pvod else pvod.multiply(r)
//      })


    if (dfActualDistMac.count() != 0) {
      for (i <- 1 to countCluster) {
//        val valueSum = sqlContext.sql(
//          """select q.mac,"""+i+""" as cluster,udfTest(sum(pvod),rand(),"""+(1==countCluster)+""") as pvod,
//            |from actualDistMac adm
//            |left join dfQ q on adm.macDist = q.mac
//            |group by adm.macDist
//          """.stripMargin)
////        timeStart = printlnDuration("for i" + i + " valueSum: " + valueSum.count(), timeStart)
////        println("for i " + i + "valueSum " + valueSum.count())

        val valueSum = dfActualDistMac
        .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
        .groupBy(col("macDist")).agg(sum("pvod").cast(DecimalType(11,10)).as("sum_pvod"))
        .withColumn("cluster", lit(i: Int).cast(IntegerType))
        .withColumn("rand", rand().cast(DecimalType(11,10)))
        .withColumn("pvod", myFunc(col("sum_pvod"), col("rand").cast(DecimalType(11,10)), lit(i == countCluster: Boolean)).cast(DecimalType(11,10)))
        val valueSum1 = valueSum.select(col("macDist").as("mac"), col("cluster"), col("pvod").cast(DecimalType(11,10)))
        timeStart = loggingDuration("for i" + i + " valueSum: " + valueSum.count() , timeStart, logger)



        valueSum.show(100)
        dfQ = dfQ.unionAll(valueSum1)
        timeStart = printlnDuration("for i" + i + " dfQ.unionAll(valueSum): " + dfQ.count() , timeStart)
      }

      println("dfQ " + dfQ.count())
      dfQ.registerTempTable("dfQ")
      dfQ.orderBy("mac").show(100)

      //val checksumDF = dfQ.groupBy(col("mac")).agg(sum("pvod").as("checksum")).filter("checksum<>1")
      val checksumDF = sqlContext.sql("select mac,sum(pvod) as checksum from dfQ group by mac having sum(pvod)<>1")
      checksumDF.show(100)
      val checksumCount = checksumDF.count()
      timeStart = printlnDuration("checksumCount - "+checksumCount , timeStart)
      if (checksumCount != 0) throw new Exception("checksumQCount != 1")

      dfQ.write.mode(SaveMode.Append).insertInto("Q")
      timeStart = loggingDuration("INSERT INTO TABLE Q - " + dfQ.count() , timeStart, logger)


    }

    //end --create dfQ
    //dfQ.show(100)
    //timeStart = loggingDuration("return dfQ - " + dfQ.count() , timeStart, logger)
     dfQ
  }

  def initQTest2_2(countCluster: Int, timeSt: DateTime): DataFrame = {

    HiveService.dropTable("initQPrimaryData")
    HiveService.createTableStbStructuredMessage(sqlContext,"initQPrimaryData")

    sqlContext.sql("INSERT INTO TABLE initQPrimaryData SELECT * FROM SbtStructuredMessage LIMIT 50")

    val logger = Logger.getLogger(getClass.getName)
    val createQ = sqlContext.sql("CREATE TABLE IF NOT EXISTS Q (" +
      "mac String," +
      "cluster Int," +
      "pvod Double" +
      ")")
    var timeStart = printlnDuration("Creating Hive table Q - " + createQ.count(), timeSt)

    //checking the count of clusters
    val dfCluster = sqlContext.sql("select distinct cluster from Q")
    if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
      logger.info("DELETE FROM Q")
      sqlContext.sql("DELETE FROM Q")
    }
    timeStart = printlnDuration("checking the count of clusters " + dfCluster.count() + "(" + countCluster + ")", timeStart)

    val dfPrimaryDataDistMac = sqlContext.sql("SELECT DISTINCT(stbStructuredMessage0.mac) as macDist from initQPrimaryData")
    println("dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count())
    dfPrimaryDataDistMac.show()
    dfPrimaryDataDistMac.registerTempTable("primaryDataDistMac")
    timeStart = printlnDuration("register temp table dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count(), timeStart)

    val dfMac = sqlContext.sql("select distinct mac from Q")
    dfMac.registerTempTable("distMac")
    timeStart = printlnDuration("register temp table dfMac " + dfMac.count(), timeStart)

    val dfActualDistMac = sqlContext.sql(
      """SELECT pd.macDist FROM primaryDataDistMac pd
        |left join distMac dm on (pd.macDist = dm.mac)
        |WHERE  dm.mac is null
      """.stripMargin)
    timeStart = printlnDuration("select actual mac: DF dfActualDistMac " + dfActualDistMac.count(), timeStart)

    dfActualDistMac.registerTempTable("actualDistMac")
    println("dfActualDistMac " + dfActualDistMac.count())
    dfActualDistMac.show()

    timeStart = printlnDuration("count dfActualDistMac: " + dfActualDistMac.count() , timeStart)
    var dfQ = sqlContext.sql("select macDist as mac,cast('1' as INT) ,round(rand(),5) as pvod from actualDistMac")

    timeStart = printlnDuration("1 dfQ " + dfQ.count() , timeStart)
    dfQ.show()

    if (dfActualDistMac.count() != 0) {
      for (i <- 2 to countCluster) {
//                        val valueSum = sqlContext.sql(
//                          """select adm.macDist as mac,"""+i+""" as cluster,rand(),sum(q.pvod) as summ,(1 - sum(q.pvod)) as pvod
//                            |from actualDistMac adm
//                            |left join dfQ q on adm.macDist = q.mac
//                            |group by adm.macDist
//                          """.stripMargin)
//        val valueSum = dfActualDistMac
//          .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
//          .groupBy(col("macDist")).agg(sum("pvod").cast(DecimalType(38, 37)).as("sum_pvod"))
//          .withColumn("cluster", lit(i: Int).cast(IntegerType))
//          .withColumn("rand", rand().cast(DecimalType(38, 37)))
//          .withColumn("pvod", myFunc(col("sum_pvod"), col("rand").cast(DecimalType(38, 37)), lit(i == countCluster: Boolean)).cast(DecimalType(38, 37)))
//          .select(col("macDist").as("mac"), col("cluster"), col("pvod"))

        val valueSum = dfActualDistMac
          .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
          .groupBy(col("macDist")).agg(sum("pvod").cast(DecimalType(6, 5)).as("sum_pvod"))
          .withColumn("cluster", lit(i: Int).cast(IntegerType))
          .withColumn("rand", rand().cast(DecimalType(6, 5)))
          .withColumn("pvod", myFunc(col("sum_pvod"), col("rand").cast(DecimalType(6, 5)), lit(i == countCluster: Boolean)).cast(DecimalType(6, 5)))
          .select(col("macDist").as("mac"), col("cluster"), col("pvod"))

        timeStart = printlnDuration("for i" + i + " valueSum: " + valueSum.count(), timeStart)
        println("for i " + i + "valueSum " + valueSum.count())
        timeStart = loggingDuration("for i" + i + " valueSum: " + valueSum.count(), timeStart, logger)
        valueSum.show(100)
        timeStart = printlnDuration("for i" + i + " dfQ.unionAll(valueSum): " + dfQ.count(), timeStart)
        dfQ = dfQ.unionAll(valueSum)
        timeStart = printlnDuration("for i" + i + " dfQ count - " + dfQ.count(), timeStart)
      }
      dfQ.registerTempTable("dfQ")
//      val valueSum2 = sqlContext.sql(
//        """select adm.macDist as mac,""" + countCluster +
//          """ as cluster,round(sum(q.pvod),5)as summ,round((1 - round(sum(q.pvod),5)),5) as pvod
//            |from actualDistMac adm
//            |left join dfQ q on adm.macDist = q.mac
//            |group by adm.macDist
//          """.stripMargin)
//      valueSum2.show()
//      dfQ = dfQ.unionAll(valueSum2.select(col("mac"), col("cluster"), col("pvod")))
      println("dfQ " + dfQ.count())
      dfQ.registerTempTable("dfQ")
      dfQ.orderBy("mac").show(200)

      //val checksumDF = dfQ.groupBy(col("mac")).agg(sum("pvod").as("checksum")).filter("checksum<>1")
      val checksumDF = sqlContext.sql("select mac,sum(cast(pvod as decimal(6,5))) as checksum from dfQ group by mac having sum(cast(pvod as decimal(6,5)))<>1")
      checksumDF.show(100)
      val checksumCount = checksumDF.count()
      timeStart = printlnDuration("checksumCount - " + checksumCount, timeStart)
      if (checksumCount != 0) throw new Exception("checksumQCount != 1")

      sqlContext.sql("INSERT INTO TABLE Q SELECT mac,cluster,pvod FROM dfQ")
      timeStart = loggingDuration("INSERT INTO TABLE Q - " + dfQ.count(), timeStart, logger)


    }

    //end --create dfQ
    //dfQ.show(100)
    //timeStart = loggingDuration("return dfQ - " + dfQ.count() , timeStart, logger)
    dfQ
  }

  def initQTest2_3(countCluster: Int, timeSt: DateTime): DataFrame = {

    //CREATE TABLE IF NOT EXISTS Q
    val createQ = HiveService.createTableWithSchemaQ("Q")
    var timeStart = printlnDuration("Creating Hive table Q - " + createQ.count(), timeSt)
    //---------------------------

    //checking the count of clusters
    val dfCluster = HiveService.checkingCountCluster(countCluster, "Q")
    timeStart = printlnDuration("checking the count of clusters " + dfCluster.count() + "(" + countCluster + ")", timeStart)
    //-----------------------------

    //INSERT INTO TABLE primaryDataDistMac
    HiveService.dropTable("primaryDataDistMac")
    HiveService.createTableWithSchemaMac("primaryDataDistMac")
    sqlContext.sql("SELECT sbtstructuredmessage0.mac as macDist from SbtStructuredMessage LIMIT 1000000").distinct().write.mode(SaveMode.Append).insertInto("primaryDataDistMac")

    val dfPrimaryDataDistMac = sqlContext.sql("select * from primaryDataDistMac")
    println("primaryDataDistMac")
    dfPrimaryDataDistMac.show()
    timeStart = printlnDuration("INSERT INTO TABLE initQPrimaryData - " + dfPrimaryDataDistMac.count(), timeSt)
    //----------------------------------

    //CREATE and INSERT table actualDistMac
    HiveService.dropTable("actualDistMac")
    HiveService.createTableWithSchemaMac("actualDistMac")

    val dfMac = sqlContext.sql("select distinct mac from Q")
    dfMac.registerTempTable("distMac")
    timeStart = printlnDuration("register temp table dfMac " + dfMac.count(), timeStart)

    sqlContext.sql(
      """INSERT INTO actualDistMac SELECT pd.mac FROM primaryDataDistMac pd
        |left join distMac dm on (pd.mac = dm.mac)
        |WHERE  dm.mac is null
      """.stripMargin)
    println("dfActualDistMac")
    val dfActualDistMac = sqlContext.sql("SELECT mac as macDist FROM actualDistMac")
      dfActualDistMac.show()
    timeStart = printlnDuration("select actual mac: DF dfActualDistMac " + dfActualDistMac.count(), timeStart)
    //-------------------------------------

    //CREATE empty dfQ
    val schemaQ = StructType(
      StructField("mac", StringType, false) ::
        StructField("cluster", IntegerType, false) ::
        StructField("pvod", DecimalType(11,10), false) :: Nil)
    var dfQ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaQ)
    //--------------------------------

    if (dfActualDistMac.count() != 0) {
      for (i <- 1 to countCluster) {
        val valueSum = dfActualDistMac
          .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
          .groupBy(col("macDist")).agg(sum("pvod").cast(DecimalType(11,10)).as("sum_pvod"))
          .withColumn("cluster", lit(i: Int).cast(IntegerType))
          .withColumn("rand", rand().cast(DecimalType(11,10)))
          .withColumn("pvod", myFunc(col("sum_pvod"), col("rand").cast(DecimalType(11,10)), lit(i == countCluster: Boolean)).cast(DecimalType(11,10)))
        valueSum.show(100)
        timeStart = loggingDuration("for i" + i + " valueSum: " + valueSum.count() , timeStart, logger)
        val valueSum1 = valueSum.select(col("macDist").as("mac"), col("cluster"), col("pvod").cast(DecimalType(11,10)))

        dfQ = dfQ.unionAll(valueSum1)

        timeStart = printlnDuration("for i" + i + " tempQ.unionAll(valueSum): " + dfQ.count() , timeStart)
      }

      println("dfQ " + dfQ.count())
      dfQ.orderBy("mac").show(100)
      dfQ.write.mode(SaveMode.Append).insertInto("Q")
      timeStart = printlnDuration("dfTempQ.write.mode " + dfQ.count() , timeStart)

      //checking sum(pvod)<>1
      val checksumDF = sqlContext.sql("select mac,sum(pvod) as checksum from tempQ group by mac having sum(pvod)<>1")
      checksumDF.show(100)
      val checksumCount = checksumDF.count()
      timeStart = printlnDuration("checksumCount - "+checksumCount , timeStart)
      if (checksumCount != 0){
        HiveService.deleteWrongMac(checksumDF, "tempQ")
        throw new Exception("checksumQCount != 1")
      }
      //---------------------
    }
    dfQ
  }

  def initQTest3(dfPrimaryData: DataFrame, countCluster: Int, timeSt: DateTime): DataFrame = {

      val logger = Logger.getLogger(getClass.getName)
      val createQ = sqlContext.sql("CREATE TABLE IF NOT EXISTS Q (" +
        "mac String," +
        "cluster Int," +
        "pvod Double" +
        ")")
      //checking the count of clusters
      val dfCluster = sqlContext.sql("select distinct cluster from Q")
      if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
        logger.info("DELETE FROM Q")
        sqlContext.sql("DELETE FROM Q")
      }

    dfPrimaryData.registerTempTable("PrimaryData")
    val dfPrimaryDataDistMac = sqlContext.sql("SELECT DISTINCT(stbStructuredMessage0.mac) as macDist from PrimaryData")
    println("dfPrimaryDataDistMac "+dfPrimaryDataDistMac.count())
    dfPrimaryDataDistMac.show()
    dfPrimaryDataDistMac.registerTempTable("primaryDataDistMac")
    //timeStart = loggingDuration("register temp table dfPrimaryDataDistMac " + dfPrimaryDataDistMac.count(), timeSt, logger)

    val dfMac = sqlContext.sql("select distinct mac from Q")
    dfMac.registerTempTable("distMac")
    //timeStart = loggingDuration("register temp table dfMac " + dfMac.count(), timeStart, logger)

    val test1 = sqlContext.sql(
      """SELECT * FROM primaryDataDistMac pd
        |left join distMac dm on (pd.macDist = dm.mac)
        |WHERE  dm.mac is null
      """.stripMargin)
    println("test")
    test1.show()
    val test2 = sqlContext.sql("select * from primaryDataDistMac")
    println("test2")
    test2.show()

    val dfActualDistMac = sqlContext.sql(
      """SELECT pd.macDist FROM primaryDataDistMac pd
        |left join distMac dm on (pd.macDist = dm.mac)
        |WHERE  dm.mac is null
      """.stripMargin)
    //timeStart = loggingDuration("select actual mac: DF dfActualDistMac " + dfActualDistMac.count() , timeStart, logger)

    println("dfActualDistMac "+dfActualDistMac.count())
    dfActualDistMac.show()

    val schemaQ = StructType(
      StructField("mac", StringType, false) ::
        StructField("cluster", IntegerType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfQ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaQ)
    /*
        if (dfActualDistMac.count() != 0) {
          timeStart = loggingDuration("count dfActualDistMac: " + dfActualDistMac.count() , timeStart, logger)
          for (i <- 1 to countCluster) {
            val valueSum = dfActualDistMac
              .join(dfQ, dfActualDistMac("macDist") === dfQ("mac"), "left_outer")
              .groupBy(col("macDist")).agg(sum("pvod").as("sum_pvod"))
              .withColumn("cluster", lit(i: Int).cast(IntegerType))
              .withColumn("rand", rand().cast(DoubleType))
              .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
              .select(col("macDist").as("mac"), col("cluster"), col("pvod"))
            timeStart = loggingDuration("for i" + i + " valueSum: " + valueSum.count() , timeStart, logger)
            valueSum.show(100)
            dfQ = dfQ.unionAll(valueSum)
            timeStart = loggingDuration("for i" + i + " dfQ.unionAll(valueSum): " + dfQ.count() , timeStart, logger)
          }
          dfQ.show(100)
          /*
          val checksumDF = dfQ.groupBy(col("mac")).agg(sum("pvod").as("checksum")).filter("checksum<>1")
          checksumDF.show(100)
          val checksumCount = checksumDF.count()
          if (checksumCount != 0) throw new Exception("checksumQCount != 1")
          timeStart = loggingDuration("checksumCount - "+checksumCount , timeStart, logger)
          dfQ.registerTempTable("dfQ")
          sqlContext.sql("INSERT INTO TABLE Q SELECT mac,cluster,pvod FROM dfQ")
          timeStart = loggingDuration("INSERT INTO TABLE Q - " + dfQ.count() , timeStart, logger)
    */
        }
        */
    //end --create dfQ
    //dfQ.show(100)
    //timeStart = loggingDuration("return dfQ - " + dfQ.count() , timeStart, logger)
    dfQ
  }
  def initJ(sc: SparkContext, sqlContext: SQLContext, dfPrimaryData: DataFrame, countCluster: Int, columnStat: Array[String]): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    //create dfJ
    logger.info("Creating Hive table J")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS J (" +
      "columnName String," +
      "value String," +
      "cluster Int," +
      "pvod Double" +
      ")")

    //checking the count of clusters
    val dfCluster = sqlContext.sql("select distinct cluster from J")
    if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
      sqlContext.sql("DELETE FROM J")
    }


    //create dfCV
    val schemaCV = StructType(
      StructField("columnName1", StringType, false) ::
        StructField("value1", StringType, false) :: Nil)
    var dfPrimaryDataDistCV = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaCV)
    logger.info("dfPrimaryDataDistCV")
    dfPrimaryDataDistCV.show()

    for (i <- 0 to columnStat.size - 1) {
      val value = dfPrimaryData.select(col(columnStat(i)).as("value")).distinct().withColumn("columnName", lit(columnStat(i): String))
      dfPrimaryDataDistCV = dfPrimaryDataDistCV.unionAll(value)
    }
    dfPrimaryDataDistCV.registerTempTable("primaryDataDistCV")
    logger.info("dfPrimaryDataDistCV")
    dfPrimaryDataDistCV.show()
    //end --create dfCV

    val dfDistCV = sqlContext.sql("select distinct columnName,value from J")
    dfDistCV.registerTempTable("distCV")
    logger.info("dfDistCV")
    dfDistCV.show()

    val dfAactualDistCV = sqlContext.sql(
      """SELECT pd.* FROM primaryDataDistCV pd
        |left join distCV dcv on (pd.columnName1 = dcv.columnName and pd.value1 = dcv.value)
        |WHERE  dcv.columnName is null
      """.stripMargin)
    logger.info("dfAactualDistCV")
    dfAactualDistCV.show()

    val schemaJ = StructType(
      StructField("cluster", IntegerType, false) ::
        StructField("columnName", StringType, false) ::
        StructField("value", StringType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfJ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaJ)

    if (dfAactualDistCV.count() != 0) {
      for (i <- 1 to countCluster) {
        val valueJ = dfPrimaryDataDistCV
          .join(dfJ, dfPrimaryDataDistCV("columnName1") === dfJ("columnName") && dfPrimaryDataDistCV("value1") === dfJ("value"), "left_outer")
          .groupBy(col("columnName1"), col("value1")).agg(sum("pvod").as("sum_pvod"))
          .withColumn("cluster", lit(i: Int).cast(IntegerType))
          .withColumn("rand", rand().cast(DoubleType))
          .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
          .select(col("columnName1").as("columnName"), col("value1").as("value"), col("cluster"), col("pvod"))
        logger.info("create context")
        valueJ.show()
        dfJ = dfJ.unionAll(valueJ)
      }
      //dfJ.show(100)
      val checksumCount = dfJ.groupBy(col("columnName"), col("value")).agg(sum("pvod").as("checksum")).filter("checksum<>1").count()
      if (checksumCount != 0) throw new Exception("checksumJCount != 1")
      //end --create dfH
    }
    logger.info("dfJ")
    dfJ.show()
    dfJ
  }

  def H(sqlContext: SQLContext, dfQ: DataFrame, dfJ: DataFrame): DataFrame = {
    val logger = Logger.getLogger(getClass.getName)
    /*
        val dfQJjoin = dfQ.join(dfJ, dfQ("cluster")===dfJ("cluster"))
        .withColumn("pvodQJ", dfQ("pvod").as("pvodQ")*dfJ("pvod").as("pvodJ"))
        dfQJjoin.show(100)
        logger.info( new DateTime()+" - "+dfQJjoin.count())
        val dfH1 = dfQJjoin.groupBy(col("mac"),col("columnName"),col("value")).agg(sum("pvodQJ"))
        dfH1.show(100)
        logger.info( new DateTime()+" - "+dfH1.count())
        */

    dfQ.registerTempTable("dfQ")
    dfJ.registerTempTable("dfJ")

    val query =
      """
                 select q.mac,q.cluster,j.columnName,j.value, j.pvod * q.pvod as pvodQJ from dfQ as q
                 left join dfJ as j on q.cluster = j.cluster
      """

    val dfH = sqlContext.sql(query)
    logger.info(dfH.count())
    dfH.show()

    dfH
  }

  def initJTest(sc: SparkContext, sqlContext: SQLContext, countCluster: Int, dfN: DataFrame, columnStat: Array[String]): DataFrame = {
    //create dfJ
    val schemaJ = StructType(
      StructField("cluster", IntegerType, false) ::
        StructField("columnName", StringType, false) ::
        StructField("value", StringType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfJ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaJ)

    val dfNDistinct = dfN.select(col("columnName"), col("value")).distinct()
    val columnNameValueDist = dfNDistinct
      .withColumn("cluster", lit(1: Int).cast(IntegerType))
      .withColumn("pvod", rand().cast(DoubleType))
      .select(col("cluster"), col("columnName"), col("value"), col("pvod"))

    dfJ = dfJ.unionAll(columnNameValueDist)

    //dfJ.show(100)
    for (i <- 2 to countCluster) {
      val valueJ = dfJ
        .groupBy(col("columnName"), col("value")).agg(sum("pvod").as("sum_pvod"))
        .withColumn("cluster", lit(i: Int).cast(IntegerType))
        .withColumn("rand", rand().cast(DoubleType))
        .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
        .select(col("cluster"), col("columnName"), col("value"), col("pvod"))
      //valueJ.show(100)
      dfJ = dfJ.unionAll(valueJ)
    }
    //dfJ.show(100)

    val checksumCount = dfJ.groupBy(col("columnName"), col("value")).agg(sum("pvod").as("checksum")).filter("checksum<>1").count()
    if (checksumCount != 0) throw new Exception("checksumJCount != 1")

    //end --create dfH
    dfJ
  }


  def myFunc = udf(
    { (c: Double, r: Double, isLastClaster: Boolean) =>
      val pvod = 1 - c
      if (isLastClaster) pvod else pvod * r
    }
  )

  def myFuncBD = udf(
    { (c: java.math.BigDecimal, r: java.math.BigDecimal, isLastClaster: Boolean) =>
      val one = new java.math.BigDecimal(1)
      val pvod = one.subtract(c)
      if (isLastClaster) pvod else pvod.multiply(r)
    }
  )

  def myFuncTest(c: java.math.BigDecimal, r: java.math.BigDecimal, isLastClaster: Boolean):java.math.BigDecimal =
  {
    val one = new java.math.BigDecimal(1)
      val pvod = one.subtract(c)
      if (isLastClaster) pvod else pvod.multiply(r)
    }

  def loggingDuration(discr: String, timeStart: DateTime, logger: Logger): DateTime = {
    val hms = new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways().appendHours().appendSeparator(":").appendMinutes().appendSuffix(":").appendSeconds().toFormatter
    val period = new Period(timeStart, new DateTime()).normalizedStandard()
    logger.info(discr + " DURATION:" + hms.print(period))
    new DateTime
  }

  def printlnDuration(discr: String, timeStart: DateTime): DateTime = {
    val hms = new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways().appendHours().appendSeparator(":").appendMinutes().appendSuffix(":").appendSeconds().toFormatter
    val period = new Period(timeStart, new DateTime()).normalizedStandard()
    println(discr + " DURATION:" + hms.print(period))
    new DateTime
  }
}
