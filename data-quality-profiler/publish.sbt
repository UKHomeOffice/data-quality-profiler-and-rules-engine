ThisBuild / organization := "io.github.6point6"
ThisBuild / organizationName := "6point6"
ThisBuild / organizationHomepage := Some(url("https://6point6.co.uk/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/6point6/data-quality-profiler-and-rules-engine"),
    "scm:git@github.com:6point6/data-quality-profiler-and-rules-engine.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "danielsmith-eu",
    name = "Dr. Daniel Alexander Smith",
    email = "dan.smith@6point6.co.uk",
    url = url("http://danielsmith.eu")
  )
)

ThisBuild / description := "Data Quality Profiler and Rules Engine."
ThisBuild / licenses := List(
  "MIT" -> new URL("https://opensource.org/license/mit/")
)
ThisBuild / homepage := Some(url("https://github.com/6point6/data-quality-profiler-and-rules-engine"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  // For accounts created after Feb 2021:
  val nexus = "https://s01.oss.sonatype.org/"
  //val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
