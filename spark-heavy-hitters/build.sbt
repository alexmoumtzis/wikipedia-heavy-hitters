ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

ThisBuild / scalacOptions --= Seq("-Wunused:imports")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql"  % "3.5.5"
)
