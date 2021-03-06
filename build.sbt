enablePlugins(JavaAppPackaging)

//logLevel := Level.Debug

name := "spark-play-park"
organization := "matlux.net"
version := "0.1"
scalaVersion := "2.11.8"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

mainClass in assembly := Some("com.rbs.frtb.service.TemplateServer")

mainClass in Compile := Some("com.rbs.frtb.service.TemplateServer")

// protocol buffer support
// seq(sbtprotobuf.ProtobufPlugin.protobufSettings: _*)
val sparkVersion = "2.1.1"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "com.typesafe" % "config" % "1.3.1"
)

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith "MANIFEST.MF" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" => MergeStrategy.discard
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last

  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => MergeStrategy.first
    //val oldStrategy = (assemblyMergeStrategy in assembly).value
    //oldStrategy(x)
}
