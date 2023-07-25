ThisBuild / organization := "uk.gov.ipt.das.dataprofiler"
ThisBuild / version      := "1.0.0"

name := "das-data-profiler-examples"

scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "das-data-profiler-examples"
  )

resolvers += Resolver.mavenLocal

lazy val sparkVersion = "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "uk.gov.ipt.das" % "das-data-profiler_2.12" % "1.0.0"
