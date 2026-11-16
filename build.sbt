scalaVersion := "2.13.18"

val akkaVersion = sys.props.getOrElse("akka.version", "2.11.0")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "org.hdrhistogram" % "HdrHistogram" % "2.2.2",
  "ch.qos.logback" % "logback-classic" % "1.5.18"
)

assembly / mainClass := Some("benchmark.Benchmark")
assembly / assemblyMergeStrategy := {
  case "reference.conf" => MergeStrategy.concat
  case path if path == "module-info.class" || path.endsWith("/module-info.class") => MergeStrategy.discard
  case path => (assembly / assemblyMergeStrategy).value(path)
}
