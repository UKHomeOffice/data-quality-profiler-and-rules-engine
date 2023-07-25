package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltInFunction
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

import java.util.regex.Pattern

case class CompoundPathExistsInRecord(definitionName: String,
                                      paths: Seq[String]
                           ) extends ProfileRule {

  private val definition: FeatureDefinition =
    FeatureDefinition(
      `type` = "DQ", name = definitionName
    )

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] =
    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        recordSet -> records.filterByPaths(filterByPaths).records.flatMap { record =>
          val profilers = paths.map { path =>

            val filter = Option(Seq(Pattern.compile(path.replace("[]", "\\[\\]"))))
            val filteredRecord = record.filterByPaths(filter)

              FeaturePoint(
                recordId = record.id,
                path = path,
                originalValue = "",
                feature = FeatureOutput(
                  feature =   FeatureDefinition(
                    `type` = "DQ", name = "PathExistsInRecord"
                  ),
                  value = BooleanValue(filteredRecord.flatValues.map(x => x.flatPath).contains(path))
                ),
                recordSet = recordSet,
                additionalIdentifiers = record.additionalIdentifiers
              )

          }

            Seq(
              FeaturePoint(
                recordId = record.id,
                path = ("CompoundPath" +: profilers.map(profiler => profiler.path).distinct).mkString("_"), 
                originalValue = ("CompoundValue" +: profilers.map(profiler => profiler.originalValue)).mkString("_"),
                feature = FeatureOutput(
                  feature = definition,
                  value = BooleanValue(!profilers.map(profiler => profiler.feature.value).contains(BooleanValue(false)))
                ),
                recordSet = recordSet,
                additionalIdentifiers = record.additionalIdentifiers
              )
            )

        }
    }

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = true
  override def allowsArrayQueryPaths:Boolean = false
}
