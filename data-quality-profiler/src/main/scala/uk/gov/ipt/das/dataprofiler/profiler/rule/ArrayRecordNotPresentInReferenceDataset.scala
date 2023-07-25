package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.profiler.input.record.{FlatValue, FlattenedRecords}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

case class ArrayRecordNotPresentInReferenceDataset(definitionName: String,
                                                   arrayPath: String,
                                                   valuePath: String,
                                                   subRecordMatch: Seq[FlatValue] => Boolean,
                                                   referenceDataset: ReferenceDataset
                               ) extends ProfileRule {

  private val definition = FeatureDefinition.fromName(definitionName)

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] = {

    val referenceDataAsSet = referenceDataset.asSet  // get ref data AsSet before entering Map.

    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        recordSet -> records.records.flatMap { record =>
          val references = record.getArray(arrayPath)

          references.flatMap { values =>
            // find sub-arrays that match the function
            if (subRecordMatch(values)) {
              // values are all values for the whole sub-object which has matched,
              // so we need to get value from the "valuePath"
              values.find(fv => fv.flatPath == valuePath).map { fv =>

                FeaturePoint(
                  recordId = record.id,
                  path = fv.flatPath,
                  originalValue = fv.recordValue.valueAsString,
                  feature = FeatureOutput(
                    feature = definition,
                    value = BooleanValue(!referenceDataAsSet.contains(fv.recordValue.valueAsString)),
                  ),
                  recordSet = recordSet,
                  additionalIdentifiers = record.additionalIdentifiers
                )
              }
            } else {
              Seq()
            }
          }
        }
    }
  }

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = true
  override def allowsArrayQueryPaths:Boolean = false
}
