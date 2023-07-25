package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

case class ValuePresentInReferenceDataset(definitionName: String,
                                          referenceDataset: ReferenceDataset) extends ProfileRule {

  private val definition: FeatureDefinition =
    FeatureDefinition(
      `type` = "DQ", name = definitionName
    )

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] = {
    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        val input = records.filterByPaths(filterByPaths)

        recordSet -> referenceDataset.df.sparkSession.sql(
          s"""SELECT ${input.viewName}.${input.idColumn}, ${input.viewName}.${input.valueColumn}, ${referenceDataset.refValue}, ${input.viewName}.${input.flatPathColumn}, ${input.viewName}.${input.additionalIdentifiersColumn}
             | FROM ${input.viewName}
             | LEFT JOIN ${referenceDataset.view} ON (${input.viewName}.${input.valueColumn} = ${referenceDataset.refValue})""".stripMargin
        ).map { row =>
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

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = true
  override def allowsArrayQueryPaths:Boolean = false
}
