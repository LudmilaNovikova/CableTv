package com.artezio.novikova.ludmila

import java.sql.Timestamp

import org.apache.spark.rdd.RDD

/**
 * Created by lnovikova on 10.02.2016.
 */
object SbtStructuredMessageService {

  def getSbtStructuredMessages(rdd: RDD[String]): RDD[SbtStructuredMessage] ={

    val format = new java.text.SimpleDateFormat("DD/MM/YYYY HH:mm:ss.SSS")
    val timeFormat = new java.text.SimpleDateFormat("mm:ss.SSS")

    rdd.map(_.split(" ")).map(r => SbtStructuredMessage(
      r(0).toInt,
      r(1),
      r(2),
      new Timestamp(format.parse(r(3) + " " + r(4)).getTime()),
      new Timestamp(timeFormat.parse(r(5)).getTime()),
      r(6),
      r(7),
      r(8).toInt,
      r(9).toInt,
      r(11),
      r(12).toInt,
      r(13).toInt,
      r(14).toInt,
      r(15).toInt,
      r(16).toInt,
      r(17).toDouble,
      r(18),
      r(19).toInt,
      r(20),
      r(21)/*,
      new Timestamp(format.parse(r(22) + " " + r(23)).getTime()),
      r(24),
      r(25),
      if(r(26).equals("X")){-1}else{r(26).toInt},
      if(r(27).equals("X")){-1}else{r(27).toInt},
      if(r(28).equals("X")){-1}else{r(28).toInt},
      if(r(29).equals("X")){-1}else{r(29).toInt},
      if(r(30).equals("X")){-1}else{r(30).toInt},
      r(32).toInt,
      r(33).toInt,
      r(34).toInt,
      r(35).toInt,
      r(36).toInt,
      r(37).toInt,
      r(38).toInt,
      r(39).toInt,
      r(40).toInt,
      r(41).toInt,
      r(42).toInt,
      r(43).toInt,
      r(44).toInt,
      r(45).toInt,
      r(46).toInt,
      r(47).toInt,
      r(48).toInt,
      r(49).toInt,
      r(50).toInt*/
    )
    )//    saveFileToHdfs(sc)

  }

}
