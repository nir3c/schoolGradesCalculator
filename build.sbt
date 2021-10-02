name := "schoolGradesCalculator"

version := "0.1"

scalaVersion := "2.13.6"

val AkkaVersion = "2.6.16"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
)