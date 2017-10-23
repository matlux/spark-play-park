name := "spark-play-park"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

// protocol buffer support
// seq(sbtprotobuf.ProtobufPlugin.protobufSettings: _*)

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.typesafe" % "config" % "1.3.1"
  //"net.sf.opencsv" % "opencsv" % "2.3"
)

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/"
)
