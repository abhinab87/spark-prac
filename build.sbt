name := "sparkprac"
version := "1.0"
scalaVersion := "2.11.7"
val sparkVersion = "2.4.3"
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "io.delta" %% "delta-core" % "0.6.1",
  "org.apache.commons" % "commons-email" % "1.2"
)