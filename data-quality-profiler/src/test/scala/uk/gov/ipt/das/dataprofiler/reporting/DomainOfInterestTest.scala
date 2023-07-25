package uk.gov.ipt.das.dataprofiler.reporting

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame
import uk.gov.ipt.das.dataprofiler.reporting.filter.{IdentifierOfInterest, PathOfInterest, RuleOfInterest}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

import scala.collection.JavaConverters._

class DomainOfInterestTest extends AnyFunSpec with SparkSessionTestWrapper {

  val schemaWithCounty: StructType = StructType(Seq(
    StructField("featurePath", StringType),
    StructField("featureName", StringType),
    StructField("featureValue", StringType),
    StructField("sampleMin", StringType),
    StructField("sampleMax", StringType),
    StructField("sampleFirst", StringType),
    StructField("sampleLast", StringType),
    StructField("count", LongType),
    StructField("county", StringType),
    StructField("year", StringType)
    // + additional identifiers if needed
  ))

  val assertionsData: MetricsDataFrame = MetricsDataFrame(
    Option(spark.createDataFrame(
      rows = Seq(
        Row("postcode", "ValidPostcode", "true", "SW1 2FR", "SW1 3FR", "SW1 4FR", "SW1 5FR", 100L, "Greater London", "2020"),
        Row("postcode", "ValidPostcode", "false", "NO POSTCODE", "NFA", "90210", "XYZ", 10L, "Hampshire", "2021"),
        Row("firstName", "MinLength2", "true", "Daniel", "Edward", "Anna", "Sam", 100L, "Surrey", "2022"),
        Row("firstName", "MinLength2", "false", "X", "X", "", "X", 5L, "Essex", "2019"),
      ).asJava,
      schema = schemaWithCounty
    ))
  )

  it("specifies a domain of interest using a RuleOfInterest") {
    val doi = DomainOfInterest(
      name = "Postcodes",
      filters = Seq(
        RuleOfInterest(ruleName = "ValidPostcode")
      )
    )

    assertionsData.dataFrame.get.show(numRows = 1000, truncate = false)

    val report = DomainOfInterestReport(domainOfInterest = doi, reportData = assertionsData)
    val reportDf = report.getReportData.dataFrame.get

    reportDf.show(numRows = 1000, truncate = false)

    val reportArr = reportDf.collect()

    assert(!reportArr.exists { row => row.getAs[String]("featureName") != "ValidPostcode" } )
  }

  it("specifies a domain of interest using multiple RuleOfInterests") {
    val doi = DomainOfInterest(
      name = "Postcodes",
      filters = Seq(
        RuleOfInterest(ruleName = "ValidPostcode"),
        RuleOfInterest(ruleName = "MinLength2"),
      )
    )

    assertionsData.dataFrame.get.show(numRows = 1000, truncate = false)

    val report = DomainOfInterestReport(domainOfInterest = doi, reportData = assertionsData)
    val reportDf = report.getReportData.dataFrame.get

    reportDf.show(numRows = 1000, truncate = false)

    val reportArr = reportDf.collect()

    assert(reportArr.length == 4)
  }

  it("specifies a domain of interest using a PathOfInterest") {
    val doi = DomainOfInterest(
      name = "Postcodes",
      filters = Seq(
        PathOfInterest(path = "postcode"),
      )
    )

    assertionsData.dataFrame.get.show(numRows = 1000, truncate = false)

    val report = DomainOfInterestReport(domainOfInterest = doi, reportData = assertionsData)
    val reportDf = report.getReportData.dataFrame.get

    reportDf.show(numRows = 1000, truncate = false)

    val reportArr = reportDf.collect()

    assert(!reportArr.exists { row => row.getAs[String]("featurePath") != "postcode" } )
  }

  it("specifies a domain of interest using multiple PathOfInterests") {
    val doi = DomainOfInterest(
      name = "Postcodes",
      filters = Seq(
        PathOfInterest(path = "postcode"),
        PathOfInterest(path = "firstName"),
      )
    )

    assertionsData.dataFrame.get.show(numRows = 1000, truncate = false)

    val report = DomainOfInterestReport(domainOfInterest = doi, reportData = assertionsData)
    val reportDf = report.getReportData.dataFrame.get

    reportDf.show(numRows = 1000, truncate = false)

    val reportArr = reportDf.collect()

    assert(reportArr.length == 4)
  }

  it("specifies a domain of interest using an IdentifierOfInterest") {
    val doi = DomainOfInterest(
      name = "Postcodes In Essex",
      filters = Seq(
        IdentifierOfInterest(identifierName = "county", identifierValue = "Essex"),
      )
    )

    assertionsData.dataFrame.get.show(numRows = 1000, truncate = false)

    val report = DomainOfInterestReport(domainOfInterest = doi, reportData = assertionsData)
    val reportDf = report.getReportData.dataFrame.get

    reportDf.show(numRows = 1000, truncate = false)

    val reportArr = reportDf.collect()

    assert(!reportArr.exists { row => row.getAs[String]("county") != "Essex" } )
  }

  it("specifies a domain of interest using multiple IdentifierOfInterests") {
    val doi = DomainOfInterest(
      name = "Postcodes In Essex and from 2021",
      filters = Seq(
        IdentifierOfInterest(identifierName = "county", identifierValue = "Essex"),
        IdentifierOfInterest(identifierName = "year", identifierValue = "2021"),
      )
    )

    assertionsData.dataFrame.get.show(numRows = 1000, truncate = false)

    val report = DomainOfInterestReport(domainOfInterest = doi, reportData = assertionsData)
    val reportDf = report.getReportData.dataFrame.get

    reportDf.show(numRows = 1000, truncate = false)

    val reportArr = reportDf.collect()

    assert(reportArr.length == 2)
    assert(reportArr.exists { row => row.getAs[String]("county") == "Essex" } )
    assert(reportArr.exists { row => row.getAs[String]("year") == "2021" } )
  }
}
