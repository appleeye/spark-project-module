name := "spark-project-module"

version := "1.0.0-SNAPSHOT"
scalaVersion := "2.11.8"

val spark_gid = "org.apache.spark"
val spark_version = "2.1.0"


transitiveClassifiers := Seq("sources")

assemblyJarName in assembly := "ad_2.10-1.0.0-SNAPSHOT.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//parallelExecution in Test := false

test in assembly := {}

val excludeServletApi = ExclusionRule(organization = "javax.servlet")
val excludeEclipseJetty = ExclusionRule(organization = "org.eclipse.jetty")
val excludeJetty = ExclusionRule(organization = "org.mortbay.jetty")


javacOptions ++= Seq("-encoding", "utf8")

javaOptions ++= Seq("-Dfile.encoding=gbk")


libraryDependencies ++= Seq(
  //*********** spark ****************
  spark_gid %% "spark-core" % spark_version,
  spark_gid %% "spark-sql" % spark_version,
  spark_gid %% "spark-streaming" % spark_version % "provided",
//  spark_gid %% "spark-streaming-kafka" % spark_version % "provided",
  spark_gid %% "spark-mllib" % spark_version % "provided",
  spark_gid %% "spark-hive" % spark_version % "provided" excludeAll(excludeEclipseJetty, excludeServletApi)
)

