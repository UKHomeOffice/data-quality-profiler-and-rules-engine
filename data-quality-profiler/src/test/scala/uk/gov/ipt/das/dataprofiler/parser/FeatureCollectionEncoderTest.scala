package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.feature.{FeatureCollection, FeatureCollectionEncoder, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class FeatureCollectionEncoderTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("encodes a featurecollection to a dataframe") {

    val fc = FeatureCollection(
      spark.createDataset(Seq(
        FeaturePoint(
          recordId = "RECORD1",
          path = "name",
          originalValue = "Dan",
          feature = FeatureOutput(
            feature = OriginalValuePassthrough.definition,
            value = StringValue("Dan")
          ),
          recordSet = "set1",
          additionalIdentifiers = AdditionalIdentifiers(
            values = List(
              AdditionalIdentifier("country" , "United Kingdom"),
              AdditionalIdentifier("age-range" , "30-45"),
            )
          )
        ),
        FeaturePoint(
          recordId = "RECORD2",
          path = "name",
          originalValue = "Sam",
          feature = FeatureOutput(
            feature = OriginalValuePassthrough.definition,
            value = StringValue("Sam")
          ),
          recordSet = "set1",
          additionalIdentifiers = AdditionalIdentifiers(
            values = List(
              AdditionalIdentifier("country" , "France"),
              AdditionalIdentifier("age-range" , "18-30"),
            )
          )
        ),
      ))
    )

    val df = FeatureCollectionEncoder.toDataFrame(fc)
    df.show()

    df.createOrReplaceTempView("fcView")

    assert(spark.sql(s"SELECT country FROM fcView WHERE recordId = 'RECORD1'")
      .first().getString(0) === "United Kingdom")
    assert(spark.sql(s"SELECT `age-range` FROM fcView WHERE recordId = 'RECORD1'")
      .first().getString(0) === "30-45")
    assert(spark.sql(s"SELECT country FROM fcView WHERE recordId = 'RECORD2'")
      .first().getString(0) === "France")
    assert(spark.sql(s"SELECT `age-range` FROM fcView WHERE recordId = 'RECORD2'")
      .first().getString(0) === "18-30")

  }

}
