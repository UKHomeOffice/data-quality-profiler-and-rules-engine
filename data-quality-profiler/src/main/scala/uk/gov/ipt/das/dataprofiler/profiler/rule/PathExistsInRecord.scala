package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

case class PathExistsInRecord(definitionName: String,
                              flatPaths: String*
                           ) extends ProfileRule {

  private val definition: FeatureDefinition =
    FeatureDefinition(
      `type` = "DQ", name = definitionName
    )

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] =
    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        recordSet -> records.records.flatMap { record =>

          val foundPaths = flatPaths.intersect(record.flatValues.map{ fv => fv.flatPath })

          flatPaths.map { flatPath =>
            FeaturePoint(
              recordId = record.id,
              path = flatPath,
              originalValue = "",
              feature = FeatureOutput(
                feature = definition,
                value = BooleanValue(foundPaths.contains(flatPath))
              ),
              recordSet = recordSet,
              additionalIdentifiers = record.additionalIdentifiers
            )
          }
        }
    }

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = false
  override def allowsArrayQueryPaths:Boolean = false
}
object PathExistsInRecord {
  /**
   * This is for TreeHugger generated scripts, as they are tricky to generate the String*
   */
   def apply(definitionName: String, flatPaths: Array[String]): PathExistsInRecord = {
     new PathExistsInRecord(definitionName, flatPaths:_*)
  }

}