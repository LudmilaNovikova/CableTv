CREATE TABLE IF NOT EXISTS SbtStructuredMessages (
      msgType String, 
      streamType String, 
      mesDate Timestamp, 
      mesInterval Timestamp, 
      mac String, 
      streamAddr String, 
      received Int, 
      linkFaults Int, 
      lostOverflow String, 
      lost Int, 
      restored Int, 
      overflow Int, 
      underflow Int, 
      mdiDf Int, 
      mdiMlr Double, 
      plc String,
      regionId Int,
      serviceAccountNumber String,
      stbIp String,
      serverDate Timestamp, 
      spyVersion String,
      playerUrl String,
      contentType Int,
      transportOuter Int,
      transportInner Int,
      channelId Int,
      playSession Int,
      scrambled Int,
      powerState Int,
      uptime Int,
      casType Int,
      casKeyTime Int,
      vidFrames Int,
      vidDecodeErrors Int,
      vidDataErrors Int,
      audFrames Int,
      audDataErrors Int,
      avTimeSkew Int,
      avPeriodSkew Int,
      bufUnderruns Int,
      bufOverruns Int,
      sdpObjectId Int,
      dvbLevelGood Int,
      dvbLevel Int,
      dvbFrequency Int,
      curBitrate Int
);