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
                                 val linkFaults: Int, //9
                                 val lostOverflow: String, //11
                                 val lost: Int, //12
                                 val restored: Int, //13
                                 val overflow: Int, //14
                                 val underflow: Int, //15
                                 val mdiDf: Int, //16
                                 val mdiMlr: Double, //17
                                 val plc: String,//18
                                 val regionId: Int,//19
                                 val serviceAccountNumber: String,//20
                                 val stbIp: String,//21
                                 val serverDate: Timestamp, //22+23
                                 val spyVersion: String//24
                                ) {

}

case class StbStructuredMessage1(
                                 val playerUrl: String,//25
                                 val contentType: Int,//26
                                 val transportOuter: Int,//27
                                 val transportInner: Int,//28
                                 val channelId: Int,//29
                                 val playSession: Int,//30
                                 val scrambled: Int,//32
                                 val powerState: Int,//33
                                 val uptime: Int,//34
                                 val casType: Int,//35
                                 val casKeyTime: Int,//36
                                 val vidFrames: Int,//37
                                 val vidDecodeErrors: Int,//38
                                 val vidDataErrors: Int,//39
                                 val audFrames: Int,//40
                                 val audDataErrors: Int,//41
                                 val avTimeSkew: Int,//42
                                 val avPeriodSkew: Int,//43
                                 val bufUnderruns: Int,//44
                                 val bufOverruns: Int//45
                                 ) {

}

case class StbStructuredMessage2(
                                  val sdpObjectId: Int,//46
                                  val dvbLevelGood: Int,//47
                                  val dvbLevel: Int,//48
                                  val dvbFrequency: Int,//49
                                  val curBitrate: Int//50
                                  ) {

}
