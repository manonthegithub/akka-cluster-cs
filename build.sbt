name := "akka-cluster-cs"

version := "0.1"

scalaVersion := "2.12.4"

val akkaVersion = "2.5.9"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)