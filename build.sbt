name := "problem"
organization := "sparkProblem"
version := "0.0.1.SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature")

//The default SBT testing java options are too small to support running many of the tests
// due to the need to launch Spark in local mode.
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false


lazy val spark = "2.0.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-mllib" % spark
)

mainClass := Some("org.apache.spark.ml.feature.Foo")
