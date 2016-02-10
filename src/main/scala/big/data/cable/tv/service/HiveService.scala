package big.data.cable.tv.service

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
/**
 * Created by lnovikova on 10.02.2016.
 */
object HiveService {

  def insertIntoTable(sqlContext: HiveContext, tableName: String, rdd: RDD[SbtStructuredMessage]): Unit ={
    import sqlContext.implicits._
    println("Going to insert: " + rdd.foreach(println(_)))
    rdd.toDF().insertInto("SbtStructuredMessages")
    //    rdd.toDF().write.mode(SaveMode.Append).insertInto("SbtStructuredMessages")
  }

  def createTableSbtStructuredMessages(sqlContext: HiveContext): Unit = {
    println("Creating Hive table SbtStructuredMessages")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS SbtStructuredMessages (" +
      "msgType String, " +
      "streamType String, " +
      "mesDate Timestamp, " +
      "mesInterval Timestamp, " +
      "mac String, " +
      "streamAddr String, " +
      "received Int, " +
      "linkFaults Int, " +
      "lostOverflow String, " +
      "lost Int, " +
      "restored Int, " +
      "overflow Int, " +
      "underflow Int, " +
      "mdiDf Int, " +
      "mdiMlr Double, " +
      "plc String," +
      "regionId Int," +
      "serviceAccountNumber String," +
      "stbIp String" + /*
      "serverDate Timestamp, " +
      "spyVersion String," +
      "playerUrl String," +
      "contentType Int," +
      "transportOuter Int," +
      "transportInner Int," +
      "channelId Int," +
      "playSession Int," +
      "scrambled Int," +
      "powerState Int," +
      "uptime Int," +
      "casType Int," +
      "casKeyTime Int," +
      "vidFrames Int," +
      "vidDecodeErrors Int," +
      "vidDataErrors Int," +
      "audFrames Int," +
      "audDataErrors Int," +
      "avTimeSkew Int," +
      "avPeriodSkew Int," +
      "bufUnderruns Int," +
      "bufOverruns Int," +
      "sdpObjectId Int," +
      "dvbLevelGood Int," +
      "dvbLevel Int," +
      "dvbFrequency Int," +
      "curBitrate Int " + */
      ")")

  }

}
