enablePlugins(JavaAppPackaging)

name := "benchmark-app"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.2.1",
  "org.apache.spark" %% "spark-mllib" % "1.2.1"
)

