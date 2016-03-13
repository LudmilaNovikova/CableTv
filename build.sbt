name := "CableTv"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.0" % "provided",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
  "javax.servlet" % "javax.servlet-api" % "3.0.1" % "provided"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)