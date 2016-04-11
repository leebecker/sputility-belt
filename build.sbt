name := "sputility-belt"

version := "1.0"

scalaVersion := "2.10.6"

// Require Java 1.8
initialize := {
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}

// Setup spark dependencies with scope=provide
lazy val sparkAndDependencies = Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"
)
libraryDependencies ++= sparkAndDependencies.map(_ % "provided")



