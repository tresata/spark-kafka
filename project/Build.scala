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
      crossScalaVersions := Seq("2.10.4", "2.11.5"),
      javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.6", "-target", "1.6"),
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.7.5" % "compile",
        "org.apache.kafka" %% "kafka" % "0.8.2.0" % "compile"
          exclude("org.jboss.netty", "netty")
          exclude("com.sun.jmx", "jmxri")
          exclude("com.sun.jdmk", "jmxtools")
          exclude("javax.jms", "jms")
          exclude("javax.mail", "mail")
          exclude("jline", "jline"),
        "org.slf4j" % "slf4j-api" % "1.6.1" % "provided",
        "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "test",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test"
      ),
      publishMavenStyle := true,
      pomIncludeRepository := { x => false },
      publishArtifact in Test := false
    )
  )
}
