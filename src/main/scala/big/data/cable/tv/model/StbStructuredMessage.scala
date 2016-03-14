package big.data.cable.tv.service

import java.sql.Timestamp

/**
 * Created by Raslu on 31.01.2016.
 */
case class StbStructuredMessage(
                                 val stbStructuredMessage0: StbStructuredMessage0, //0
                                 val stbStructuredMessage1: StbStructuredMessage1, //0
                                 val stbStructuredMessage2: StbStructuredMessage2 //0
                                 ) {

}

case class StbStructuredMessage0(
                                 val counter: Int, //0
                                 val msgType: String, //1
                                 val streamType: String, //2
                                 val date: Timestamp, //3+4 date + time
                                 val interval: Timestamp, //5
                                 val mac: String, //6
                                 val streamAddr: String, //7
                                 val received: Int, //8
                                 val linkFaults: Int, //
                                 val lostOverflow: String, //
                                 val lost: Int, //
                                 val restored: Int, //
                                 val overflow: Int, //
                                 val underflow: Int, //
                                 val mdiDf: Int, //
                                 val mdiMlr: Double, //17
                                 val plc: String,
                                 val regionId: Int,
                                 val serviceAccountNumber: String,
                                 val stbIp: String,
                                 val serverDate: Timestamp, //22+23
                                 val spyVersion: String
                                ) {

}

case class StbStructuredMessage1(
                                 val playerUrl: String,
                                 val contentType: Int,
                                 val transportOuter: Int,
                                 val transportInner: Int,
                                 val channelId: Int,
                                 val playSession: Int,
                                 val scrambled: Int,
                                 val powerState: Int,
                                 val uptime: Int,
                                 val casType: Int,
                                 val casKeyTime: Int,
                                 val vidFrames: Int,
                                 val vidDecodeErrors: Int,
                                 val vidDataErrors: Int,
                                 val audFrames: Int,
                                 val audDataErrors: Int,
                                 val avTimeSkew: Int,
                                 val avPeriodSkew: Int,
                                 val bufUnderruns: Int,
                                 val bufOverruns: Int
                                 ) {

}

case class StbStructuredMessage2(
                                  val sdpObjectId: Int,
                                  val dvbLevelGood: Int,
                                  val dvbLevel: Int,
                                  val dvbFrequency: Int,
                                  val curBitrate: Int
                                  ) {

}
