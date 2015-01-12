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
      version := "0.2",
      scalaVersion := "2.10.4",
      javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.6", "-target", "1.6"),
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.6"),
      libraryDependencies ++= Seq(
        "org.slf4j" % "slf4j-api" % "1.6.6" % "compile",
        "org.apache.kafka" %% "kafka" % "0.8.1.1" % "compile"
          exclude("org.jboss.netty", "netty")
          exclude("com.sun.jmx", "jmxri")
          exclude("com.sun.jdmk", "jmxtools")
          exclude("javax.jms", "jms")
          exclude("javax.mail", "mail")
          exclude("jline", "jline"),
        "org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
        "org.slf4j" % "slf4j-log4j12" % "1.6.6" % "test",
        "org.scalatest" %% "scalatest" % "2.2.0" % "test"
      ),
      publishMavenStyle := true,
      pomIncludeRepository := { x => false },
      publishArtifact in Test := false,
      publishTo <<= version { (v: String) =>
        if (v.trim.endsWith("SNAPSHOT"))
          Some("tresata-snapshots" at "http://server01:8080/archiva/repository/snapshots")
        else
          Some("tresata-releases"  at "http://server01:8080/archiva/repository/internal")
      },
      credentials += Credentials(Path.userHome / ".m2" / "credentials_internal"),
      credentials += Credentials(Path.userHome / ".m2" / "credentials_snapshots"),
      credentials += Credentials(Path.userHome / ".m2" / "credentials_proxy")
    )
  )
}
