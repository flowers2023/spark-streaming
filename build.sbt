name := "spark-streaming"

version := "1.0"

scalaVersion := "2.10.3"

val sparkVersion = "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % sparkVersion

libraryDependencies += "redis.clients" % "jedis" % "2.4.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.1.1"
