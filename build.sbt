import com.typesafe.sbt.SbtStartScript

seq(SbtStartScript.startScriptForClassesSettings: _*)

name := "mqtt-loadtest"

version := "0.1-SNAPSHOT"

organization := "io.m2m.mqtt"

scalaVersion := "2.10.2"

scalacOptions += "-deprecation"

resolvers ++= Seq(
	"Maven Repository" at "http://repo1.maven.org/maven2/",
	"Typsafe" at "http://repo.typesafe.com/typesafe/releases",
	"sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases",
	"Eclipse" at "https://repo.eclipse.org/content/repositories/paho-releases/",
	"scct-github-repository" at "http://mtkopone.github.com/scct/maven-repo"
)

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-actor" % "2.2.0",
  "com.typesafe.akka" %% "akka-remote" % "2.2.0",
	"com.typesafe" % "config" % "1.0.2",
	"org.scala-lang" % "scala-reflect" % "2.10.2",
	"org.joda" % "joda-convert" % "1.4",
	"joda-time" % "joda-time" % "2.3",
	"org.fusesource.mqtt-client" % "mqtt-client" % "1.5",
	"org.json4s" %% "json4s-native" % "3.1.0",
	"org.scalatest" %% "scalatest" % "2.0.M5b" % "test",
	"org.mashupbots.socko" %% "socko-webserver" % "0.3.0",
	"net.databinder.dispatch" %% "dispatch-core" % "0.11.0",
	"ch.qos.logback" % "logback-classic" %"1.0.1",
	"net.sf.xenqtt" % "xenqtt" % "0.9.3"
)
