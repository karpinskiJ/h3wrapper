name := "h3xwrapper"
version := "1.0.0"
scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.sedona" %% "sedona-spark-shaded-3.0" % "1.5.0" % "provided",
  "org.datasyslab" % "geotools-wrapper" % "1.5.0-28.2" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.uber" % "h3" % "4.1.1"
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
