package big.data.cable.tv.service

import big.data.cable.tv.STBStatistics._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
/**
 * Created by lnovikova on 10.02.2016.
 */
object HiveService {

  def insertIntoTable(sqlContext: HiveContext, tableName: String, rdd: RDD[StbStructuredMessage]): Unit ={
    import sqlContext.implicits._
//    println("Going to insert: " + rdd.foreach(println(_)))
//    rdd.toDF().insertInto("StbStructuredMessages")
    rdd.toDF().write.mode(SaveMode.Append).insertInto(tableName)
  }

  def insertIntoTableDF(tableName: String, df: DataFrame): Unit ={
    println("insertInto table "+tableName)
    df.write.mode(SaveMode.Append).insertInto(tableName)
  }

  def createTableStbStructuredMessage(sqlContext: HiveContext, tableName: String): Unit = {
    println("Creating Hive table " + tableName)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
      "stbStructuredMessage0 struct <" +
      "counter: Int," +
      "msgType: String," +
      "streamType: String," +
      "mesDate: Timestamp," +
      "mesInterval: Timestamp," +
      "mac: String," +
      "streamAddr: String," +
      "received: Int," +
      "linkFaults: Int," +
      "lostOverflow: String," +
      "lost: Int," +
      "restored: Int," +
      "overflow: Int," +
      "underflow: Int," +
      "mdiDf: Int," +
      "mdiMlr: Double," +
      "plc: String," +
      "regionId: Int," +
      "serviceAccountNumber: String," +
      "stbIp: String," +
      "serverDate: Timestamp," +
      "spyVersion: String>," +
      "stbStructuredMessage1 struct <" +
      "playerUrl: String," +
      "contentType: Int," +
      "transportOuter: Int," +
      "transportInner: Int," +
      "channelId: Int," +
      "playSession: Int," +
      "scrambled: Int," +
      "powerState: Int," +
      "uptime: Int," +
      "casType: Int," +
      "casKeyTime: Int," +
      "vidFrames: Int," +
      "vidDecodeErrors: Int," +
      "vidDataErrors: Int," +
      "audFrames: Int," +
      "audDataErrors: Int," +
      "avTimeSkew: Int," +
      "avPeriodSkew: Int," +
      "bufUnderruns: Int," +
      "bufOverruns: Int>," +
      "stbStructuredMessage2 struct <" +
      "sdpObjectId: Int," +
      "dvbLevelGood: Int," +
      "dvbLevel: Int," +
      "dvbFrequency: Int," +
      "curBitrate: Int>" +
      ")")

  }

  def dropTable(tableName: String): DataFrame = {
    println("DROP TABLE IF EXISTS "+tableName)
    sqlContext.sql("DROP TABLE IF EXISTS "+tableName)
  }

  def createTableWithSchemaQ(tableName: String):DataFrame = {
    sqlContext.sql("CREATE TABLE IF NOT EXISTS "+tableName+" (" +
      "mac String," +
      "cluster Int," +
      "pvod Decimal(11,10)" +
      ")")
    sqlContext.sql("select * from "+tableName)
  }

  def checkingCountCluster(countCluster: Int, tableName: String): DataFrame = {
    val dfCluster = sqlContext.sql("SELECT DISTINCT cluster from "+tableName)
    if (dfCluster.count() != 0 && dfCluster.count() != countCluster) {
      sqlContext.sql("DELETE FROM "+tableName)
    }
    dfCluster
  }

  def createTableWithSchemaMac(tableName:String):DataFrame = {
    sqlContext.sql("CREATE TABLE IF NOT EXISTS "+tableName+" (mac String)")
  }

  def insertIntoActualDistMac(tableNameFrom: String):DataFrame = {

    println("tableNameFrom "+tableNameFrom)
    sqlContext.sql("SELECT * FROM "+tableNameFrom).show(200)

    println("test***/")
    val test = sqlContext.sql(
      """WITH m1 AS (SELECT DISTINCT stbStructuredMessage0.mac as mac from """ + tableNameFrom +"""),
          |m2 AS (SELECT DISTINCT mac from Q)
          |select * from m1 left join m2 on m1.mac=m2.mac
          |where m2.mac is null
        """.stripMargin)
    test.show()
    println("test count "+test.count())


    val dfActualDistMac = sqlContext.sql(
      """with m1 AS (SELECT DISTINCT stbStructuredMessage0.mac as mac from """+tableNameFrom+"""),
        |m2 AS (SELECT DISTINCT mac from Q)
        |insert overwrite table actualDistMac
        |select m1.mac from m1 left join m2 on m1.mac=m2.mac
        |where m2.mac is null
      """.stripMargin)

/*
    sqlContext.sql("INSERT INTO actualDistMac SELECT DISTINCT stbStructuredMessage0.mac as mac from "+tableNameFrom)
    val dfActualDistMac = sqlContext.sql(
      """WITH m1 AS (SELECT DISTINCT mac from Q)
        |DELETE FROM actualDistMac
        |WHERE  mac=m1.mac
      """.stripMargin)
*/
    sqlContext.sql("SELECT * FROM actualDistMac")
  }

  def deleteWrongMac(dfWrongMac: DataFrame, tableName:String):Unit = {
    dfWrongMac.registerTempTable("wrongMac")
    sqlContext.sql(
      """DELETE FROM  """+tableName+"""
        |WHERE  mac is (SELECT DISTINCT mac from wrongMac)
      """.stripMargin)
  }


  def createTablePrimaryData():Unit = {
    sqlContext.sql("SELECT * from SbtStructuredMessage LIMIT 1000000").write.mode(SaveMode.Append).insertInto("primaryData")
 }

  def createTableWithSchemaJ(tableName: String):DataFrame = {
    sqlContext.sql("CREATE TABLE IF NOT EXISTS "+tableName+" (" +
      "cluster Int," +
      "columnName String," +
      "value String," +
      "pvod Decimal(11,10)" +
      ")")
  }

  def createTableWithSchemaCNV(tableName: String):DataFrame = {
    sqlContext.sql("CREATE TABLE IF NOT EXISTS "+tableName+" (" +
      "columnName String," +
      "value String"+
      ")")
  }

  def createTableUsers(nameTable : String):DataFrame = {
    println("CREATE TABLE IF NOT EXISTS "+nameTable)
    sqlContext.sql("CREATE TABLE IF NOT EXISTS "+nameTable+" (" +
      "mac String," +
      "cluster Int," +
      "count Int"+
      ")")
  }

  def createTableClusters(nameTable : String):DataFrame = {
    sqlContext.sql("CREATE TABLE IF NOT EXISTS "+nameTable+" (" +
      "cluster Int," +
      "col Int," +
      "value float"+
      ")")
  }
}
