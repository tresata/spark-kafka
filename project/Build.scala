import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.Plugin._

object ProjectBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = Project.defaultSettings ++ graphSettings ++ Seq(
      name := "spark-kafka",
      organization := "com.tresata",
      version := "0.3.0-SNAPSHOT",
      scalaVersion := "2.10.4",
      javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.6", "-target", "1.6"),
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.7.5" % "compile",
        "org.apache.kafka" %% "kafka" % "0.8.1.1" % "compile"
          exclude("com.101tec", "zkclient")
          exclude("org.apache.zookeeper", "zookeeper"),
        "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
        "com.101tec" % "zkclient" % "0.3" % "test"
          exclude("org.jboss.netty", "netty"),
        "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "test",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test"
      ),
      publishMavenStyle := true,
      pomIncludeRepository := { x => false },
      publishArtifact in Test := false
    )
  )
}
