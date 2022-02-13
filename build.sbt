name := "tokenizer"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.13" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.13" % sparkVersion
)
