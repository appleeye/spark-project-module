name := "ad"

version := "1.0.0-SNAPSHOT"
scalaVersion := "2.11.8"

val spark_gid = "org.apache.spark"
val spark_version = "2.1.0"


transitiveClassifiers := Seq("sources")

assemblyJarName in assembly := "ad_2.10-1.0.0-SNAPSHOT.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

parallelExecution in Test := false

test in assembly := {}

val excludeServletApi = ExclusionRule(organization = "javax.servlet")
val excludeEclipseJetty = ExclusionRule(organization = "org.eclipse.jetty")
val excludeJetty = ExclusionRule(organization = "org.mortbay.jetty")

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Sonatype OSS Snapshots Repository" at "http://oss.sonatype.org/content/groups/public",
  "Apache" at "http://maven.apache.org",
  "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
  //"AWS release" at "http://maven-us.everstring.com:8081/nexus/content/repositories/releases",
  //"BJ release" at "http://maven-bj.everstring.net:8081/nexus/content/repositories/releases",
  "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
)

javacOptions ++= Seq("-encoding", "utf8")

javaOptions ++= Seq("-Dfile.encoding=gbk")

// disable publishing the main API jar
publishArtifact in(Compile, packageDoc) := false

// disable publishing the main sources jar
publishArtifact in(Compile, packageSrc) := false

assemblyMergeStrategy in assembly := {
  case "application.conf" => MergeStrategy.concat
  case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.discard
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case f if f.matches("META-INF.*\\.(SF|DSA|RSA)$") => MergeStrategy.discard
  case x =>
    //val oldStrategy = (assemblyMergeStrategy in assembly).value
    //oldStrategy(x)
    MergeStrategy.last
}

resourceDirectory in Compile <<= baseDirectory(_ / "src/main/resources")

libraryDependencies ++= Seq(
  //*********** test only ****************
  "org.mockito" % "mockito-core" % "1.8.5" % "test",
  "org.scala-lang" % "scala-library" % "2.10.6",
  "junit" % "junit" % "4.10",
  "org.scalatest" %% "scalatest" % "2.2.4",
  "org.scalacheck" %% "scalacheck" % "1.12.4" % "test",
  "com.holdenkarau" % "spark-testing-base_2.10" % "1.5.1_0.2.0",
  "org.scalaj" % "scalaj-http_2.10" % "2.2.1",
  "net.liftweb" %% "lift-json" % "2.6.3",
  "org.scala-lang" % "scala-reflect" % "2.10.6",
  //*********** spark ****************
  spark_gid %% "spark-core" % spark_version % "provided" withSources(),
  spark_gid %% "spark-streaming" % spark_version % "provided",
  spark_gid %% "spark-streaming-kafka" % spark_version % "provided",
  spark_gid %% "spark-mllib" % spark_version % "provided",
  spark_gid %% "spark-hive" % spark_version % "provided" excludeAll(excludeEclipseJetty, excludeServletApi),
  "com.databricks" % "spark-csv_2.10" % "1.2.0",
  // scala datetime
  "com.github.nscala-time" %% "nscala-time" % "2.10.0",
  "log4j" % "log4j" % "1.2.14",
  "io.spray" %% "spray-json" % "1.3.2",
  // maxmind ip2location
  "com.maxmind.geoip2"  % "geoip2"          % "2.3.1",
  "com.twitter"        %% "util-collection" % "6.23.0"
)

(testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")

dependencyOverrides += "com.google.guava" % "guava" % "16.0.1"

dependencyOverrides += "org.apache.commons" % "commons-lang3" % "3.4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

libraryDependencies += "commons-codec" % "commons-codec" % "1.10"

libraryDependencies += "org.apache.commons" % "commons-parent" % "35"

