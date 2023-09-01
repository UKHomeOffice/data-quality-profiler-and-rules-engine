ThisBuild / organization := "io.github.6point6"
ThisBuild / version      := "1.0.0"

name := "data-quality-profiler-and-rules-engine"

scalaVersion := "2.12.15"

lazy val sparkVersion = "3.1.1"


libraryDependencies += "org.apache.hadoop" % "hadoop-client" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-funspec" % "3.2.10" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % sparkVersion % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % sparkVersion % "provided"

libraryDependencies += "com.crealytics" %% "spark-excel" % "0.14.0"

libraryDependencies += "org.rogach" %% "scallop" % "4.0.4"

libraryDependencies += "org.apache.logging.log4j" % "log4j" % "2.16.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.16.0"

libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "2.2.1" % "test" // JSoup for HTML report tests

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.5"

// for Spark 3
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"

val jacksonVersion = "2.13.3"

// we have to manully bump the version of jackson, so that we don't get this error:
// Caused by: com.fasterxml.jackson.databind.JsonMappingException: Scala module 2.10.0 requires Jackson Databind version >= 2.10.0 and < 2.11.0
// but we don't use jackson in our code, it's used by Spark libs.
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion


// YAML decoding
libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion


libraryDependencies += "com.dslplatform" %% "dsl-json-scala" % "1.9.9"

libraryDependencies += "org.scalameta" %% "scalafmt-dynamic" % "3.3.2"

// https://mvnrepository.com/artifact/org.reflections/reflections
libraryDependencies += "org.reflections" % "reflections" % "0.10.2"


// https://mvnrepository.com/artifact/com.itextpdf/html2pdf
libraryDependencies += "com.itextpdf" % "html2pdf" % "4.0.3"

// https://mvnrepository.com/artifact/com.github.spullara.mustache.java/compiler
libraryDependencies += "com.github.spullara.mustache.java" % "compiler" % "0.9.10"

//avro support
libraryDependencies += "org.apache.avro" % "avro" % "1.11.1"


Test / parallelExecution := true

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// these are all to allow local publishing to overwrite the version
/** make a fat jar */
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)
/***/


// these are all to allow local publishing to overwrite the version
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)

Test / publishArtifact := false


Compile / compile / wartremoverErrors ++= Seq(Wart.IterableOps, Wart.Throw, Wart.Null, Wart.Var, Wart.Return, Wart.OptionPartial, Wart.Any)
Compile / compile / wartremoverExcluded += (LocalRootProject / baseDirectory).value / "src" / "test"

Test / publishArtifact := false

Test / parallelExecution := false
coverageMinimumStmtTotal := 90
coverageMinimumBranchTotal := 90
coverageMinimumStmtPerPackage := 90
coverageMinimumBranchPerPackage := 85
coverageMinimumStmtPerFile := 85
coverageMinimumBranchPerFile := 80

coverageExcludedPackages := "<empty>;.*AssertionFunctions;" +
  ".*JobConfigFileAsString;.*GlueFunctions;.*RulesFileAsString;.*AggregateMetrics;.*S3ListPreficesIterator;.*ListableAmazonS3Client;.*SourceDataPartitionerGlue;" +
  ".*SourceDataPartitioner;.*SourceDataPartitionerGlue;.*GlueCompaniesHouse;.*InitGlue;.*InitGlueMetricsDf;.*InitLocal"


initialize := {
  val _ = initialize.value // run the previous initialization
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}

ThisBuild / scalacOptions ++=Seq("-JXss512m","-JXmx8G")

