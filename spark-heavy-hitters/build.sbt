ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

ThisBuild / scalacOptions --= Seq("-Wunused:imports")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql"  % "3.5.5",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// ── local-cluster executor jar ───────────────────────────────────────────────
// `sbt package` builds a thin jar of just the project classes (no Spark deps).
// Executors already have Spark on their classpath; they only need project code.
// Run `sbt package` once before running partition benchmarks.
