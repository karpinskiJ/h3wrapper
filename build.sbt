name:="h3xwrapper"
version:= "0.0.0"
scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
//  "org.apache.sedona" %% "sedona-python-adapter-3.0" % "1.4.1",
//  "org.apache.sedona" %% "sedona-viz-3.0" % "1.4.1",
  "org.apache.sedona" %% "sedona-spark-shaded-3.0" % "1.5.0",
  "org.datasyslab" % "geotools-wrapper" % "1.5.0-28.2",
"org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.uber" % "h3" % "4.1.1"
)