val sparkVersion = sys.props.getOrElse("spark.version", "1.6.2")
val connectorVersion = sys.props.getOrElse("connector.version", "1.6.2")

name := "spark-cassandra-connector-benchmarks"
version := "0.1"
scalaVersion := sys.props.getOrElse("scala.version", "2.10.6")
sbtVersion := "0.13.12"

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % connectorVersion,
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion)

lazy val benchmarks = (project in file("."))
  .enablePlugins(JmhPlugin)
