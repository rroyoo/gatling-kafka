name := "gatling-kafka"

organization := "com.github.mnogu"

version := "0.3.2-SNAPSHOT"

scalaVersion := "2.13.5"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "io.gatling" % "gatling-core" % "3.5.1" % "provided",
  ("org.apache.kafka" % "kafka-clients" % "2.7.0")
    // Gatling contains slf4j-api
    .exclude("org.slf4j", "slf4j-api"),
  "org.scalatest" %% "scalatest" % "3.2.8" % Test,
  "org.scalamock" %% "scalamock" % "5.0.0" % Test,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % "2.6.11" % Test,
  "org.apache.avro" % "avro" % "1.10.2" % Test,
  "io.confluent" % "kafka-avro-serializer" % "6.1.1" % Test
)

// Gatling contains scala-library
assemblyOption in assembly := (assemblyOption in assembly).value
  .copy(includeScala = false)
