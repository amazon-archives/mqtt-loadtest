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
	"org.eclipse.paho" % "mqtt-client" % "0.4.0",
	"joda-time" % "joda-time" % "2.3",
	"org.joda" % "joda-convert" % "1.4"
)
