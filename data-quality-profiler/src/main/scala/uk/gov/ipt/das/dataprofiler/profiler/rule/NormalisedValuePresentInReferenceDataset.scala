package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

case class NormalisedValuePresentInReferenceDataset(definitionName: String,
                                                    referenceDataset: ReferenceDataset) extends ProfileRule {

  private val definition: FeatureDefinition = {
      FeatureDefinition(
        `type` = "DQ_NORMALISED", name = definitionName
      )
  }

  private val trimUpperUDF = referenceDataset.df.sparkSession.udf.register("trimUpperUDF",trimUpper)

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] = {
    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        val input = records.filterByPaths(filterByPaths)

        recordSet -> referenceDataset.df.sparkSession.sql(
          s"""SELECT ${input.viewName}.${input.idColumn},
             | ${input.viewName}.${input.valueColumn},
             | ${referenceDataset.refValue},
             | ${input.viewName}.${input.flatPathColumn},
             | ${input.viewName}.${input.additionalIdentifiersColumn}
             | FROM ${input.viewName}
             | LEFT JOIN ${referenceDataset.view} ON (trimUpperUDF(${input.viewName}.${input.valueColumn}) =
             | trimUpperUDF(${referenceDataset.refValue}))""".stripMargin ).map { row =>
          val (recordId, originalValue, referenceValue, flatPath, additionalIdentifiers: AdditionalIdentifiers) =
            (row.getString(0), row.getString(1), row.getString(2), row.getString(3), AdditionalIdentifiers.decodeFromString(row.getString(4)))

          FeaturePoint(
            recordId = recordId,
            path = flatPath,
            originalValue = originalValue,
            feature = FeatureOutput(
              feature = definition,
              value = BooleanValue(referenceValue != null)
            ),
            recordSet = recordSet,
            additionalIdentifiers = additionalIdentifiers
          )
        }
    }
  }

  private def trimUpper : String => String = (str: String) =>
    str.toUpperCase.trim.replaceAll("\\s+", "")

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = true
  override def allowsArrayQueryPaths:Boolean = false
}
