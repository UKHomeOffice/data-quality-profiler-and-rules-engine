package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.value.RecordValue

case class CompareTwoFields(definitionName: String,
                            pathOne: String,
                            pathTwo: String,
                            comparisonFunction: (RecordValue, RecordValue) => String
                           ) extends ProfileRule {

  private val definition: FeatureDefinition =
    FeatureDefinition(
      `type` = "DQ", name = definitionName
    )

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] =
    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        recordSet -> records.records.flatMap { record =>
          val valueOneOpt = record.flatValues.find{ fv => fv.flatPath == pathOne }.map{ _.recordValue }
          val valueTwoOpt = record.flatValues.find{ fv => fv.flatPath == pathTwo }.map{ _.recordValue }

          (valueOneOpt, valueTwoOpt) match {
            case (Some(valueOne), Some(valueTwo)) =>
              Option(FeaturePoint(
                recordId = record.id,
                path = pathOne,
                originalValue = valueOne.valueAsString, // TODO implement RecordValue originalValues ?
                feature = FeatureOutput(
                  feature = definition,
                  value = RecordValue.fromAny(comparisonFunction(valueOne, valueTwo))
                ),
                recordSet = recordSet,
                additionalIdentifiers = record.additionalIdentifiers
              ))
            case _ =>
              None
          }
        }
    }

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = false
  override def allowsArrayQueryPaths:Boolean = false
}
