name := "scala_redshift"
version := "1.0"
scalaVersion := "2.11.8"
val sparkVersion="2.2.2"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "jitpack" at "https://jitpack.io"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "io.netty" % "netty" % "3.9.9.Final",
  "com.typesafe" % "config" % "1.3.3",
  "com.github.databricks" %% "spark-redshift" % "master-SNAPSHOT"
)

